import numpy as np
import tensorflow as tf

# https://blog.csdn.net/huhu0769/article/details/71169346


def test_embedding_lookup():
    a = np.arange(8).reshape(2, 4)
    b = np.arange(8, 12).reshape(1, 4)
    c = np.arange(12, 20).reshape(2, 4)
    # print(a)
    # print(b)
    # print(c)

    a = tf.Variable(a)
    b = tf.Variable(b)
    c = tf.Variable(c)

    t = tf.nn.embedding_lookup([a, b, c], ids=[0, 1, 2, 3])
    '''
    [[ 0  1  2  3]
    [ 8  9 10 11]
    [12 13 14 15]
    [ 4  5  6  7]]
    '''

    init = tf.global_variables_initializer()
    sess = tf.Session()
    sess.run(init)
    m = sess.run(t)
    print(m)


if __name__ == "__main__":
    test_embedding_lookup()
