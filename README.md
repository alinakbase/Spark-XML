# Spark-XML
This script reads UniRef cluster XML files in batch using Spark-XML, 
extracts structured cluster-level fields (such as representative accession, sequence, 
member count, and common taxon ID), and writes the results to Delta Lake tables.
It supports robust parsing for various nested and heterogeneous XML structures,
producing both cluster-level and member-level tables for downstream analytics.
