def parallelize_mul(df):
    df['output'] = df.apply(mul_row_2, axis=1)
    return df

def mul_row_2(row):
    return row['A'] * row['B'] * row['C'] * row['D']