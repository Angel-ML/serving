import tensorflow as tf
from tensorflow.python import debug as tf_debug
from dataset import DataSet

num_epoch, batch_size, dim = 100, 128, 123
x_data = tf.placeholder(dtype=tf.float32, shape=(None, dim), name="x_data")
labels = tf.placeholder(dtype=tf.float32, shape=(None, 1), name="labels")

with tf.variable_scope("variables"):
    weight = tf.get_variable(name="weight", shape=(dim, 1), dtype=tf.float32,
                             initializer=tf.truncated_normal_initializer(stddev=0.001))
    bias = tf.get_variable(name="bias", shape=[], dtype=tf.float32)
    global_steps = tf.get_variable(name="global_steps", shape=[], dtype=tf.int32, trainable=False)

logits = tf.matmul(x_data, weight) + bias
loss = tf.reduce_mean(tf.nn.sigmoid_cross_entropy_with_logits(logits=logits, labels=labels), name="loss_mean")
optimizer = tf.train.GradientDescentOptimizer(learning_rate=0.1)

train_op = optimizer.minimize(loss, global_steps)

init = tf.global_variables_initializer()

with tf.Session() as sess:
    sess = tf_debug.LocalCLIDebugWrapperSession(sess)
    sess.run(init)

    writer = tf.summary.FileWriter("logs", graph=sess.graph)
    writer.flush()
    tdata = DataSet.read_dense("data/a9a/a9a_123d_train.dense")
    dataset = DataSet(tdata, num_epoch, batch_size)
    for data_x, data_y in dataset:
        loss_, gs, _ = sess.run([loss, global_steps, train_op], feed_dict={
            x_data: data_x,
            labels: data_y
        })

        if gs % 100 == 0:
            print("{gs:10d}\t{loss}".format(gs=gs, loss=loss_))
    writer.close()
