from mxnet import npx, np

x = np.array([1, 2, 4, 8])
y = np.array([2, 2, 2, 2])

print(x + y, x * y)
print(np.exp(x))