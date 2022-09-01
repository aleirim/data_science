import re
import pickle
import pandas as pd
import numpy as np

from unicodedata import normalize
from nltk.corpus import stopwords
from sklearn.feature_extraction.text import TfidfVectorizer
from scipy.sparse import hstack

with open(f'model/tfidf_name.pkl', 'rb') as vec1:
    tfidf_name = pickle.load(vec1)
with open(f'model/tfidf_text.pkl', 'rb') as vec2:
    tfidf_text = pickle.load(vec2)
with open(f'model/ohe_condition.pkl', 'rb') as ohe1:
    ohe_cond = pickle.load(ohe1)
with open(f'model/ohe_shiping.pkl', 'rb') as ohe2:
    ohe_ship = pickle.load(ohe2)

condition_map = {'New':1, 'Like New':2, 'Good':3, 'Fair':4, 'Poor':5}
shipping_map = {'Seller': 1, 'Buyer':0}

def preprocess_data(datas_as_df):
    print("--------- Preprocessing Data ----------")
    # map item condition
    datas_as_df['item_condition_id'] = datas_as_df.item_condition_id.map(condition_map)
    print("Item Condition ID:", datas_as_df['item_condition_id'].values[0])
    # map shipping
    datas_as_df['shipping'] = datas_as_df.shipping.map(shipping_map)
    print("Shipping ID:", datas_as_df['shipping'].values[0])
    # clean name
    datas_as_df['name'] = datas_as_df.name.map(pre_process_name)
    print("Name:", datas_as_df['name'].values[0])
    # clean description
    datas_as_df['item_description'] = datas_as_df.item_description.map(pre_process_description)
    print("Description:", datas_as_df['item_description'].values[0])
    # clean brand_name
    datas_as_df['brand_name'] = datas_as_df.brand_name.map(pre_process_brand)
    print("Brand:", datas_as_df['brand_name'].values[0])
    # clean categories
    print("Original category:", datas_as_df['category_name'].values[0])
    print("Expected category process:", pre_process_category(datas_as_df['category_name'].values[0]))
    datas_as_df['main_cat'], datas_as_df['sub_cat1'], datas_as_df['sub_cat2'] = zip(*datas_as_df['category_name'].apply(lambda x: pre_process_category(x)))
    # joining text features
    datas_as_df['name_comb'] = datas_as_df['name'] + ' ' + datas_as_df['brand_name']
    datas_as_df['text'] = datas_as_df['name'] + ' ' + datas_as_df['item_description'] + ' ' + datas_as_df['main_cat'] + ' ' + datas_as_df['sub_cat1'] + ' ' + datas_as_df['sub_cat2']
    
    # encoding data for model
    # 1. encoding text
    data_name_vec = tfidf_name.transform(datas_as_df.name_comb)
    data_text_vec = tfidf_text.transform(datas_as_df.text)
    # 2. encoding categorical data with OHE
    data_cond_ohe = ohe_cond.transform(datas_as_df.item_condition_id.values.reshape(-1,1))
    data_ship_ohe = ohe_ship.transform(datas_as_df.shipping.values.reshape(-1,1))

    # stacking data
    data_matrix = hstack((data_name_vec, data_text_vec, data_cond_ohe, data_ship_ohe))
    print("--------- Finished Preprocessing Data ----------")
    return data_matrix

# vectorize tet using TfidfVectorizer
def vectorize_text(text:str):
    return text

def encode_ohe(value):
    return value

def get_pred_price(log_price):
    print("Log price prediction:", log_price)
    return np.exp(log_price)-1

def split_category(text, sep):
    try:
        cat_list = text.split(sep)
        if len(cat_list) >= 3:
            return (cat_list[0], cat_list[1], cat_list[2])
        elif len(cat_list) >= 2:
            return (cat_list[0], cat_list[1], '')
        else:
            return (cat_list[0], '', '')
    except:
        return ('', '', '')

def normalize_text(text):
    '''
    Handles diacritic marks, superscripts and subscripts.
    Returns the text in lowercase.
    '''
    text = text.replace('–', '-').strip()
    text = normalize('NFKD', text).encode('ascii', 'ignore').decode("utf-8").lower()
    return text

def remove_stopwords(text:str):
    stop = stopwords.words('english')
    text = ' '.join([word for word in text.split() if word not in (stop)])
    return text

def expand_contractions(text):
    text = re.sub(r"won\'t", "will not", text)
    text = re.sub(r"can\'t", "can not", text)

    # general
    text = re.sub(r"n\'t", " not", text)
    text = re.sub(r"\'re", " are", text)
    text = re.sub(r"\'s", " is", text)
    text = re.sub(r"\'d", " would", text)
    text = re.sub(r"\'ll", " will", text)
    text = re.sub(r"\'t", " not", text)
    text = re.sub(r"\'ve", " have", text)
    text = re.sub(r"\'m", " am", text)
    return text

def pre_process_name(text:str):
    text = expand_contractions(text)
    clean_text = normalize_text(text)
    clean_text = re.sub('[^A-Za-z0-9\/]+', ' ', clean_text)
    # removing stopwords
    clean_text = remove_stopwords(clean_text)
    if(len(clean_text)==0):
        clean_text = ''
    return clean_text

def pre_process_description(text):
    text = expand_contractions(text)
    clean_text = normalize_text(text)
    clean_text = re.sub("\[rm\] ","", str(clean_text))
    clean_text = re.sub('[^A-Za-z0-9\/]+', ' ', clean_text)
    clean_text = remove_stopwords(clean_text)
    if(len(clean_text)==0):
        clean_text = ''
    return clean_text

def pre_process_brand(text):
    clean_text = normalize_text(text).replace(' ', '_')
    return clean_text

def pre_process_category(text):
    clean_text = normalize_text(text)
    clean_text = re.sub('[^A-Za-z0-9\/]+', ' ', clean_text).replace(' ', '_')
    categ = split_category(clean_text, '/')
    return categ
