import tensorflow as tf


num_epoch, batch_size, dim = 1000, 128, 6
x_data = tf.placeholder(dtype=tf.float32, shape=(None, dim), name="x_data")
labels = tf.placeholder(dtype=tf.float32, shape=(None, 1), name="labels")

# with tf.variable_scope("variables"):
weight = tf.get_variable(name="weight", shape=(dim, 1), dtype=tf.float32,
                         initializer=tf.truncated_normal_initializer(stddev=0.001))
bias = tf.get_variable(name="bias", shape=[], dtype=tf.float32)
global_steps = tf.get_variable(name="global_steps", shape=[], dtype=tf.int32, trainable=False)

diff = tf.matmul(x_data, weight) + bias - labels
loss = tf.nn.l2_loss(diff)
optimizer = tf.train.GradientDescentOptimizer(learning_rate=0.01)

train_op = optimizer.minimize(loss, global_steps)

init = tf.global_variables_initializer()
with tf.Session() as sess:
    sess.run(init)

    writer = tf.summary.FileWriter("logs", graph=sess.graph)
    writer.close()
