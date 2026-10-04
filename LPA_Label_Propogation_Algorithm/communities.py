import pandas as pd
pd.options.display.width = 0

class GetComm():

    def __init__(self):
        pass

    def get_comm(self):
        self.df = pd.read_csv('communities/part-00000-29490f6e-284f-47a5-9fae-00894ddca6a1-c000.csv')
        # print(self.df.sort_values(by='label',ascending=True).head())
        self.df = self.df.groupby('label').agg({'url':'count'}).reset_index()
        print(self.df.sort_values(by='url',ascending=False).head(20))

    def get_items(self):
        self.df = pd.read_csv('communities/part-00000-29490f6e-284f-47a5-9fae-00894ddca6a1-c000.csv')
        self.df = self.df.loc[self.df['label'] == 1494648621359]
        print(self.df.head(20))

if __name__ == "__main__":
    gcom = GetComm()
    # gcom.get_comm()
    gcom.get_items()