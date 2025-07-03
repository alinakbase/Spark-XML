import os
import shutil
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import col, udf, explode
from pyspark.sql.types import StringType, IntegerType


# Set up the Spark Session with Delta Lake support
def create_spark_session():
    builder = (
        SparkSession.builder.appName("UniRef Spark XML Pipeline")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


# General utility: Safely get dict or Row attributes (mixed XML parser returns)
def get_attr(obj, key):
    if isinstance(obj, dict):
        return obj.get(key)
    return getattr(obj, key, None)


# Extract UniProtKB accession from the representative member (main protein of the cluster)
@udf(returnType=StringType())
def extract_rep_accession(rep_member):
    """
    Try to get UniProtKB accession from representativeMember > dbReference > property[type=UniProtKB accession].
    Falls back to dbReference id if not present.
    Handles both dict and Row as well as possible list structures.
    """
    if not rep_member:
        return None
    dbref = (
        rep_member.get("dbReference")
        if isinstance(rep_member, dict)
        else getattr(rep_member, "dbReference", None)
    )
    if isinstance(dbref, list):
        for db in dbref:
            props = (
                db.get("property")
                if isinstance(db, dict)
                else getattr(db, "property", None)
            )
            props = props if isinstance(props, list) else [props] if props else []
            for p in props:
                if get_attr(p, "type") == "UniProtKB accession":
                    val = get_attr(p, "value")
                    if val:
                        return val
            fallback = db.get("id") if isinstance(db, dict) else getattr(db, "id", None)
            if fallback:
                return fallback
        return None
    elif isinstance(dbref, dict):
        props = dbref.get("property")
        if props:
            props = props if isinstance(props, list) else [props]
            for p in props:
                if get_attr(p, "type") == "UniProtKB accession":
                    val = get_attr(p, "value")
                    if val:
                        return val
        fallback = dbref.get("id")
        if fallback:
            return fallback
    elif dbref:
        # Handle Row style
        props = getattr(dbref, "property", None)
        if props:
            props = props if isinstance(props, list) else [props]
            for p in props:
                if getattr(p, "type", None) == "UniProtKB accession":
                    val = getattr(p, "value", None)
                    if val:
                        return val
        fallback = getattr(dbref, "id", None)
        if fallback:
            return fallback
    return None


# Extract the amino acid sequence for the representative member
@udf(returnType=StringType())
def extract_rep_sequence(rep_member):
    """
    Get protein sequence from representativeMember > sequence.
    Handles possible dict/Row/str types in XML struct.
    """
    if not rep_member:
        return None
    seq = (
        rep_member.get("sequence")
        if isinstance(rep_member, dict)
        else getattr(rep_member, "sequence", None)
    )
    if not seq:
        return None
    if isinstance(seq, dict):
        return seq.get("_VALUE") or seq.get("text") or seq.get("_text")
    if hasattr(seq, "_VALUE"):
        return getattr(seq, "_VALUE")
    if hasattr(seq, "text"):
        return getattr(seq, "text")
    if isinstance(seq, str):
        return seq
    return None


# Extract number of cluster members from top-level property list
@udf(returnType=IntegerType())
def extract_member_count(properties):
    """
    Returns the 'member count' integer for the cluster, if available.
    """
    if not properties:
        return None
    props = properties if isinstance(properties, list) else [properties]
    for prop in props:
        if (
            get_attr(prop, "_type") == "member count"
            or get_attr(prop, "type") == "member count"
        ):
            v = get_attr(prop, "_value") or get_attr(prop, "value")
            try:
                return int(v)
            except Exception:
                return v
    return None


# Extract common taxonomy ID for the cluster
@udf(returnType=IntegerType())
def extract_common_taxon_id(properties):
    """
    Returns the common NCBI taxonomy ID for the cluster, if available.
    """
    if not properties:
        return None
    props = properties if isinstance(properties, list) else [properties]
    for prop in props:
        if (
            get_attr(prop, "_type") == "common taxon ID"
            or get_attr(prop, "type") == "common taxon ID"
        ):
            v = get_attr(prop, "_value") or get_attr(prop, "value")
            try:
                return int(v)
            except Exception:
                return v
    return None


def main():
    # Set paths and Delta destinations
    input_dir = "ARCHAEA_UNIPROT"
    delta_output = "uniref_spark_delta"
    db_name = "uniref_db"
    table_name = "uniref_clusters"
    members_delta_output = "uniref_spark_delta"

    spark = create_spark_session()
    xml_path = os.path.join(input_dir, "*.xml")

    # Read all UniRef cluster XMLs as a Spark DataFrame (entry = one cluster)
    df = spark.read.format("xml").option("rowTag", "entry").load(xml_path)

    # Project and enrich DataFrame with parsed fields for downstream analytics
    df = (
        df.withColumn(
            "rep_accession", extract_rep_accession(col("representativeMember"))
        )
        .withColumn("rep_sequence", extract_rep_sequence(col("representativeMember")))
        .withColumn("member_count", extract_member_count(col("property")))
        .withColumn("common_taxon_id", extract_common_taxon_id(col("property")))
    )

    # Write the cluster-level Delta table (one row per cluster)
    if os.path.exists(delta_output):
        shutil.rmtree(delta_output)
    df.write.format("delta").mode("overwrite").save(delta_output)

    # Optionally: Also flatten out the members and write a member-level Delta table for granular analytics
    df_members = df.select(
        col("_id").alias("cluster_id"),
        col("_updated").alias("updated"),
        col("name"),
        col("rep_accession"),
        col("rep_sequence"),
        col("member_count"),
        col("common_taxon_id"),
        explode("member").alias("member"),
    )
    if os.path.exists(members_delta_output):
        shutil.rmtree(members_delta_output)
    df_members.write.format("delta").mode("overwrite").save(members_delta_output)

    # Register cluster table in Spark SQL catalog for SQL access
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {db_name}.{table_name}
        USING DELTA
        LOCATION '{os.path.abspath(delta_output)}'
    """)

    # Display a few records for quick validation (sample 10 clusters)
    spark.sql(f"""
        SELECT cluster_id, name, updated, rep_accession, rep_sequence, member_count, common_taxon_id
        FROM {db_name}.{table_name}
        LIMIT 10
    """).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
