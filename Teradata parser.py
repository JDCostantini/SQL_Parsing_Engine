import pandas as pd
import numpy as np
import re

def tables_in_query(sql_str):

    # remove the /* */ comments
    q = re.sub(r"/\*[^*]*\*+(?:[^*/][^*]*\*+)*/", "", sql_str)

    # remove whole line -- and # comments
    lines = [line for line in q.splitlines() if not re.match("^\s*(--|#)", line)]

    # remove trailing -- and # comments
    q = " ".join([re.split("--|#", line)[0] for line in lines])

    # add space between commas if not space existis 
    q = re.sub(r'[,]', r', ', q)
    
    # split on blanks, parens and semicolons
    tokens = re.split(r"[\s)(;]+", q)

    # scan the tokens. if we see a FROM or JOIN, we set the get_next
    # flag, and grab the next one (unless it's SELECT).

    result = []
    get_next = False
    from_ident = False
    for tok in tokens:
        comma = ',' in tok
        if (tok.lower() in ['from']):
            from_ident = True
        elif (tok.lower() in ['select','order','where','group','having','limit','in']):
            from_ident= False
#         print(tok,'-', get_next,'-', comma, '-', from_ident)
        if (get_next & from_ident):
            if tok.lower() not in ["", "select"]:
#                 print('tok to be added', tok)
                tok =re.sub(r'[,]', r'', tok)
#                 print('tok to be added', tok)
                result.append(tok)
        if comma:
            get_next = True
        else:
            get_next = tok.lower() in ["from", "join"]
#     print(result)
    return result

### Test of above function ###
column = ['user', 'sql_query']
list_of_lists = [
    ['user1', 'CREATE TABLE EDWARD_FIX_WGS_STAR AS SELECT group_id ,MBRSHP_SOR_CD ,legal_entity as LEGAL_ENTITY2 ,line_of_business AS LINE_OF_BUSINESS2 ,mbu AS MBU2 ,funding as funding2 ,MAX(service_date) AS max_date FROM final2 WHERE LEGAL_ENTITY NOT IN (‘UNK’,‘’) AND MBU NOT IN (‘UNK’,‘’) AND substr(MBU,1,2) NOT IN (‘LG’) AND mbrshp_sor_cd IN (‘808’,‘815’,‘877’)   /*  WGS STAR  QCARE_NOTREADY  */ GROUP BY 1,2,3,4,5 ORDER BY 1,2,7 DESC'],
    ['user2', 'SELECT * FROM table.table1 LEFT JOIN table2;'],
    ['user3', 'SELECT ID FROM Customers INNER JOIN Orders'],
    ['','select K.a,K.b from (select H.b from H), I, J, K join L on order by 1, 2;'],
]
sql_query_dataframe = pd.DataFrame(list_of_lists, columns=column)
sql_query_dataframe['tables']= ''
sql_query_dataframe.head()
sql_query_dataframe.shape

for index, row in sql_query_dataframe.iterrows():
    row['tables']= tables_in_query(row.sql_query)
sql_query_dataframe.to_csv('results.csv')  
