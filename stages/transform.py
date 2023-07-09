def transform(local_df, db_df, s3_df):
    new_local_df, local_removed = _identify_duplicates(local_df)
    new_db_df, db_removed = _identify_duplicates(db_df)
    new_s3_df, S3_removed = _identify_duplicates(s3_df)
    new_local_df = _order_by_title(new_local_df)
    new_db_df = _order_by_title(new_db_df)
    new_s3_df = _order_by_title(new_s3_df)
    
    if _check_title_order(new_local_df, new_db_df, new_s3_df) == True:
        final_df = _merge_dfs(new_local_df, new_db_df, new_s3_df, 'Title')
        
        return final_df
    
    elif _check_title_order(new_local_df, new_db_df, new_s3_df) == False:
        
        return "Data is not consistent across data sources"


### HELPER FUNCTIONS ###

def _identify_duplicates(df):
    # Identify duplicate rows based on the "Title" column
    duplicates = df[df.duplicated(subset='Title', keep=False)]

    # Remove duplicates from DataFrame
    new_df = df.drop_duplicates(subset='Title', keep=False)

    # Create a DataFrame with only the removed duplicate rows
    removed_df = df[df.duplicated(subset='Title', keep=False)]

    # Return the new DataFrame with duplicate rows removed and the DataFrame with the removed rows
    return new_df, removed_df

def _order_by_title(df):
    # Sort the DataFrame alphabetically on the "Title" column
    sorted_df = df.sort_values(by='Title')

    # Reset the index of the sorted DataFrame
    sorted_df = sorted_df.reset_index(drop=True)

    # Return the sorted DataFrame
    return sorted_df

def _check_title_order(local_df, db_df, s3_df):
    local_df_titles = local_df['Title'].tolist()
    db_df_titles = db_df['Title'].tolist()
    s3_df_titles = s3_df['Title'].tolist()

    if local_df_titles == db_df_titles == s3_df_titles:
        return True
    else:
        return False
    
def _merge_dfs(local_df, db_df, s3_df, merge_key):
    final_df = local_df.merge(db_df, on=merge_key).merge(s3_df, on=merge_key)
    
    return final_df