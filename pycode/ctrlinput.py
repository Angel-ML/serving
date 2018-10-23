import tensorflow as tf

a = tf.constant(0, dtype=tf.float32, name="a")
b = tf.constant(1, dtype=tf.float32, name="b")
c = tf.constant(2, dtype=tf.float32, name="c")

with tf.control_dependencies([a, c]):
    d = tf.add(a, b, name="d")
    e = tf.subtract(b, c, name="e")
    f = tf.multiply(d, e, name="f")
    with tf.control_dependencies([d, f]):
        g = tf.floordiv(b, d, name="g")

init = tf.global_variables_initializer()
with tf.Session() as sess:
    sess.run(init)

    writer = tf.summary.FileWriter("logs", sess.graph)
    print(g.eval())
    writer.close()
    print()

