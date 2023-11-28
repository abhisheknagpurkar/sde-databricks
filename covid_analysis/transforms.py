import pandas as pd

# Filter by country code. If not specified, use "USA."
def filter_country(pdf, country="USA"):
    pdf = pdf[pdf.iso_code == country]
    return pdf

def pivot_and_clean(pdf, fillna):
    pdf['value'] = pd.to_numeric(pdf['value'])
    pdf = pdf.fillna(fillna).pivot_table(
        values="value", columns="indicator", index="date"
    )
    return pdf

def clean_spark_cols(pdf):
    pdf.columns = pdf.columns.str.replace(' ', '_')
    return pdf

def index_to_col(df, colname):
    df[colname] = df.index
    return df

