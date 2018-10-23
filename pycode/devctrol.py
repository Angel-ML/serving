import tensorflow as tf

with tf.device("/replica:0/task:0"):
    a = tf.constant(0, dtype=tf.float32, name="a")
    b = tf.constant(2, dtype=tf.float32, name="b")
    with tf.device("/job:localhost/device:CPU:1"):
        c = tf.constant(3, dtype=tf.float32, name="c")
        with tf.device("/replica:1/device:CPU:0"):
            d = tf.constant(3, dtype=tf.float32, name="d")


init = tf.global_variables_initializer()
with tf.Session() as sess:
    sess.run(init)
    writer = tf.summary.FileWriter("logs", sess.graph)
    print(d.eval())
    writer.close()

print()
