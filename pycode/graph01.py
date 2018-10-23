import tensorflow as tf
import numpy as np


# math_ops: _OverrideBinaryOperatorHelper
a_np = np.array([[1, 2], [3, 4], [6, 7], [10, 32]], dtype=np.float32)
b_np = np.array([[1, 6], [7, 12], [78, 25], [33, 50]], dtype=np.int32).T

a_tf = tf.cast(tf.constant(a_np, tf.float32, shape=a_np.shape), dtype=tf.int32, name="cast")
b_tf = tf.constant(b_np, tf.int32, shape=b_np.shape)
c_tf = tf.constant([[1, 34], [2, 16], [21, 26], [83, 57]], tf.int32, shape=a_np.shape)
d_tf = tf.constant([1, 34, 2, 16, 21, 26, 83, 57], tf.int32, shape=(8,))
a = tf.constant(2)
b = tf.constant(3)
c = tf.constant(3)

add = a_tf + c_tf
mul = a_tf * c_tf
div = a_tf / c_tf
div2 = a_tf // c_tf
mod = a_tf % c_tf
logical = tf.cond((a >= b) | (c < a), lambda: tf.matmul(add, abs(b_tf)), lambda: - (a_tf - c_tf) ** 3)
# or_ = a_tf or c_tf
# not_ = not c_tf
# inv_ = ~c_tf
gt = a_tf > c_tf
ge = a_tf >= c_tf
lt = a_tf < c_tf
le = a_tf <= c_tf

# [and_, or_, not_, inv_]
ops = [mul, d_tf, div, div2, mod, gt, lt, ge, le, logical]
with tf.control_dependencies(ops):
    noOp = tf.no_op(name="NoOp")

with tf.Session() as sess:
    writer = tf.summary.FileWriter("logs", sess.graph)
    sess.run(noOp)
    writer.close()

print(noOp.graph.as_graph_def(add_shapes=True))
print()
