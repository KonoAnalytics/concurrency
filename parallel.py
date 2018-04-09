import multiprocessing
import pandas as pd
import numpy as np

def worker(procnum, return_dict):
    '''worker function'''
    print(str(procnum) + ' represent!')
    return_dict[procnum] = pd.DataFrame(np.random.randint(0,100,size=(5, 4)), columns=list('ABCD'))


if __name__ == '__main__':
    manager = multiprocessing.Manager()
    return_dict = manager.dict()
    jobs = []
    for i in range(5):
        p = multiprocessing.Process(target=worker, args=(i,return_dict))
        jobs.append(p)
        p.start()

    for proc in jobs:
        proc.join()
    unk = return_dict.values()
    print(type(unk))
    for i in range(5):
        if i == 0:
            df_out = unk[i]
        else:
            df_out = df_out.append(unk[i])
    print(df_out)
