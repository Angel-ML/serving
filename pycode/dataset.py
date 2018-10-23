import numpy as np
from random import shuffle


class DataSet(object):
    def __init__(self, data, num_epoch: int = 100, batch_size: int = 128, num_class: int = 2, use_one_hot=False):
        self._data = data
        self._num_epoch = num_epoch
        self._batch_size = batch_size
        self._num_class = num_class
        self._use_one_hot = use_one_hot
        self._num_batch = len(data) // batch_size

        self._epoch_count = 0
        self._batch_count = 0
        self._indices = list(range(len(data)))

    @staticmethod
    def read_libsvm(fname, num_feats, use_zero=True):
        with open(fname, 'r') as fid:
            data = []
            for line in fid:
                feat = [0.0] * (num_feats + 1)
                ll = line.strip().split()

                if use_zero and float(ll[0]) < 0:
                    feat[0] = 0
                else:
                    feat[0] = float(ll[0])

                for item in ll[1:]:
                    k, v = item.split(":")
                    feat[int(k)] = float(v)
                data.append(feat)

        return np.array(data, dtype=np.float32)

    @staticmethod
    def read_dummy(fname, num_feats, use_zero=True):
        with open(fname, 'r') as fid:
            data = []
            for line in fid:
                feat = [0.0] * (num_feats + 1)
                ll = line.strip().split()

                if use_zero and float(ll[0]) < 0:
                    feat[0] = 0
                else:
                    feat[0] = float(ll[0])

                for item in ll[1:]:
                    feat[int(item) + 1] = 1.0
                data.append(feat)

        return np.array(data, dtype=np.float32)

    @staticmethod
    def read_dense(fname, use_zero=True):
        with open(fname, 'r') as fid:
            data = []
            for line in fid:
                feat = [float(item) for item in line.strip().split()]
                if use_zero and float(feat[0]) < 0:
                    feat[0] = 0
                else:
                    feat[0] = float(feat[0])
                data.append(feat)

        return np.array(data, dtype=np.float32)

    def __iter__(self):
        return self

    def __next__(self):
        if self._epoch_count >= self._num_epoch:
            raise StopIteration()

        if self._batch_count == self._num_batch:
            self._batch_count = 0
            self._epoch_count += 1
            shuffle(self._indices)

        start = self._batch_size * self._batch_count
        end = self._batch_size * (self._batch_count + 1)
        self._batch_count += 1

        batch = self._data[self._indices[start:end]]
        x_data = batch[:, 1:]

        if not self._use_one_hot:
            y_data = np.array(batch[:, 0].reshape(self._batch_size, 1))
        else:
            hot_idxs = [self._num_class * i + v for i, v in enumerate(batch[:, 0])]
            labels = np.zeros(shape=(self._batch_size * self._num_class, ))
            labels[hot_idxs] = 1
            y_data = labels.reshape(self._batch_size, self._num_class)

        return x_data, y_data


if __name__ == '__main__':
    tdata = DataSet.read_dense("data/a9a/a9a_123d_train.dense")
    ds = DataSet(tdata)
    for x, y in ds:
        print(x, y)
