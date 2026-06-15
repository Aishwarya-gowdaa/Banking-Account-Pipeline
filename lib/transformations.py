from pyspark.sql.functions import lit, col, struct, array, when, isnotnull, filter, collect_list,current_timestamp,expr


def get_insert_ops(column, alias):
    return struct(lit("INSERT").alias("Operations"), column.alias("newValue"), lit(None).alias("oldvalue")).alias(alias)


def get_trans_accounts(accounts_df):
    legal = array(
        when(col("legal_title_1").isNotNull(), struct(lit("lgl_ttl_ln_1").alias("contractTitleLineType"),
                                                      col("legal_title_1").alias("contractTitleLine"))),
        when(col("legal_title_2").isNotNull(), struct(lit("lgl_ttl_ln_2").alias("contractTitleLineType"),
                                                      col("legal_title_2").alias("contractTitleLine"))))

    lgl_ttl = filter(legal, lambda x: isnotnull(x))

    tax = struct(
        col("tax_id_type").alias("taxIdType"),
        col("tax_id").alias("taxId")
    )

    return accounts_df.select("account_id",
                              get_insert_ops(col("account_id"), "contractIdentifier"),
                              get_insert_ops(col("source_sys"), "sourceSystemIdentifier"),
                              get_insert_ops(col("account_start_date"), "contractStartDateTime"),
                              get_insert_ops(lgl_ttl, "contractTitle"),
                              get_insert_ops(tax, "taxIdentifier"),
                              get_insert_ops(col("branch_code"), "contractBranchCode"),
                              get_insert_ops(col("country"), "contractCountry")
                              )


def get_trans_parties(parties_df):
    return parties_df.select("party_id", "account_id",
                             get_insert_ops(col("party_id"), "partyIdentifier"),
                             get_insert_ops(col("relation_type"), "partyRelationshipType"),
                             get_insert_ops(col("relation_start_date"), "partyRelationStartDateTime"))


def get_trans_address(address_df):
    addrs = struct(col("address_line_1").alias("addressLine1"),
                   col("address_line_2").alias("addressLine2"),
                   col("city").alias("addressCity"),
                   col("postal_code").alias("addressPostalCode"),
                   col("country_of_address").alias("addressCountry"),
                   col("address_start_date").alias("addressStartDate"))

    return address_df.select("party_id",
                             get_insert_ops(addrs, "partyAddress"))


def get_join_parties(pdf, padf):
    return pdf.join(padf, "party_id", "left").groupBy("account_id").agg(collect_list(struct(
        "partyIdentifier",
        "partyRelationshipType",
        "partyRelationStartDateTime",
        "partyAddress")).alias("partyRelations"))

def get_join_address(adf, padf):
    return adf.hint("broadcast").join(padf,"account_id","left")

def get_final_df(fdf):
    final_df = fdf.select(
        struct(
            expr("uuid()").alias("eventIdentifier"),
            lit("SBDL-Contract").alias("eventType"),
            lit(1).alias("majorSchemaVersion"),
            lit(0).alias("minorSchemaVersion"),
            current_timestamp().alias("eventDateTime")
        ).alias("eventHeader"),
        array(struct(lit("contractIdentifier").alias("keyField"), col("account_id").alias("keyValue"))).alias("keys"),
        struct("contractIdentifier", "sourceSystemIdentifier", "contractStartDateTime", "contractTitle",
               "contractBranchCode", "contractCountry", "taxIdentifier", "partyRelations").alias("payload"))
    return final_df