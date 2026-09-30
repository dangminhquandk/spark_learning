import pandas as pd
import time, glob
import numpy as np

class Wikilinks():

    def __init__(self):
        pass

    def read_file_pandas(self):
        self.files = glob.glob('/media/sid/0EFA13150EFA1315/dnm/wikilinks/data-00009-of-00010')
        self.counter = 0
        self.data = {
            'url' : {

            },
            'mention' : {

            }
        }
        for file in self.files:
            for line in open(file):
                # if self.counter >= 100:
                #     break
            #     print(line)
                self.url = ''
                if line.startswith("URL"):
                    self.url = line.split("\t")[1].split("\n")[0]
                    self.url_part = { self.counter : self.url }
                    self.data['url'].update(self.url_part)

                if line.startswith("MENTION"):

                    self.mention = line.split("\t")[-1].split('/')[-1].split("\n")[0]
                    self.mention_part = { self.counter : self.mention }
                    self.url_part = {self.counter: self.url}
                    self.data['mention'].update(self.mention_part)
                    self.data['url'].update(self.url_part)

                self.counter += 1

        self.df = pd.DataFrame()
        self.df =  self.df.from_dict(self.data)
        self.df['url'] = self.df['url'].replace('',np.nan)
        self.df['mention'] = self.df['mention'].fillna(method='bfill')
        self.df['url'].mask(self.df == 'nan', None).fillna(method='ffill',inplace=True)
        print(self.df)
        self.df = self.df.drop_duplicates(keep='first')
        self.df.to_csv('results_new/' + str(file).split('/')[-1] + ".csv",sep=',',index=False)

if __name__ == '__main__':
    wiki = Wikilinks()
    # wiki.read_files()
    wiki.read_file_pandas()