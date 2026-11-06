from helpers import clean_customers


def run(session, source_table, target_table):
    raw = session.table(source_table)
    clean = clean_customers(raw)
    clean.write.save_as_table(target_table, mode="overwrite")

    # pandas comes from Environment(packages=...) in the sample
    emails = session.table(target_table).to_pandas()["EMAIL"]
    by_domain = emails.str.split("@").str[1].value_counts().to_dict()

    read = raw.count()
    written = session.table(target_table).count()
    return {
        "read": read,
        "written": written,
        "dropped": read - written,
        "by_domain": by_domain,
    }
