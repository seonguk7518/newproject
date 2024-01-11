import pymysql.cursors
from pymysql.connections import Connection


class DBAdapter:
    '''
    2023/12/15 - pysonic
    DB(mysql) INFO 
    - DB name : chart_data
    - table name : signal_data
    db adapter extract LONG/SHORT signal coin list (received datetime(str) argument)
    ex) db_adapter.extract_S_signal_coin_list('2023-12-15 14:00:00')
    '''
    def __init__(self):
        self._get_db_connection_info()

    def _get_connection(self):
        try:
            connection = Connection(host=self.HOST,
                                    user=self.USER,
                                    password=self.PASSWORD,
                                    database=self.DATABASE,
                                    cursorclass=pymysql.cursors.DictCursor)
            connection.ping(False)
        except Exception as e:
            print("connection error", e)
        else:
            return connection

    def _get_db_connection_info(self) -> None:
        self.HOST: str = "52.79.226.104"
        self.USER: str = "datateam"
        self.PASSWORD: str = "datateam1234$$"
        self.DATABASE: str = "chart_data"

    def select_signal(self, df_datetime: str) -> list:
        conn = self._get_connection()

        if conn:
            try:
                with conn.cursor() as cursor:
                    fucking_literal = "[\'\', \'\']"
                    
                    sql = "SELECT * FROM chart_data.signal_data where basesignal != %s and datetime = %s;"
                    cursor.execute(sql, (fucking_literal, df_datetime))
                    result = cursor.fetchall()
                
                return result
            except Exception as e:  # TODO: Custom Exception
                print("select signal error", e)
                
        return []
            
    def extract_S_signal_coin_list(self, now_df_time: str) -> list:
        
        short_coin_list: list = []
        
        FUCKING_LITERAL1 = "['', 'S1']"
        FUCKING_LITERAL2 = "['', 'S2']"
        FUCKING_LITERAL3 = "['', 'S3']"
        FUCKING_LITERAL_LIST = [FUCKING_LITERAL1, FUCKING_LITERAL2, FUCKING_LITERAL3]
        
        signal_result = self.select_signal(now_df_time)
        
        for signal in signal_result:
            for LITERAL in FUCKING_LITERAL_LIST:
                if signal['basesignal'] == LITERAL:
                    short_coin_list.append(signal['ticker'])
                    break
                    
        return short_coin_list
        
    def extract_L_signal_coin_list(self, now_df_time: str) -> list:
        
        long_coin_list: list = []
        
        FUCKING_LITERAL1 = "['', 'L1']"
        FUCKING_LITERAL2 = "['', 'L2']"
        FUCKING_LITERAL3 = "['', 'L3']"
        FUCKING_LITERAL_LIST = [FUCKING_LITERAL1, FUCKING_LITERAL2, FUCKING_LITERAL3]
        
        signal_result = self.select_signal(now_df_time)
        
        for signal in signal_result:
            for LITERAL in FUCKING_LITERAL_LIST:
                if signal['basesignal'] == LITERAL:
                    long_coin_list.append(signal['ticker'])
                    break
                    
        return long_coin_list

