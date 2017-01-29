import numpy as np

# weighting function on scalar x. takes in alpha and x_max parameters.
def f(x, alpha=float(3/4), x_max=100):
    if x < x_max:
        return float(x / x_max) ** alpha
    else:
        return 1

# cost function summed over cooccurence matrix X, represented as a V x V
# numpy array, where V is the size of the vocabulary.
# w and w_tilde are V x d arrays, giving d-length vectors for each word.
# b and b_tilde are vectors of length V, giving bias terms for each word.
def cost(X, w, w_tilde, b, b_tilde):
    J = 0
    for i in xrange(X.shape[0]):
        for j in xrange(X.shape[1]):
            resid = w[i].dot(w_tilde[j]) + b[i] + b_tilde[j] - np.log(X[i,j])
            J += f(X[i,j]) * resid ** 2
    return J
