from math import sqrt


# Calculate the mean value of a list of numbers
def mean(values):
    return sum(values) / float(len(values))


# Calculate covariance between x and y
def covariance(x, mean_x, y, mean_y):
    covar = 0.0
    for i in range(len(x)):
        covar += (x[i] - mean_x) * (y[i] - mean_y)
    return covar


# Calculate the variance of a list of numbers
def variance(values, mean):
    return sum([(x - mean) ** 2 for x in values])


# Calculate coefficients
def coefficients(x, y):
    x_mean, y_mean = mean(x), mean(y)
    b1 = covariance(x, x_mean, y, y_mean) / variance(x, x_mean)
    b0 = y_mean - b1 * x_mean
    return [b0, b1]


# Simple linear regression algorithm
def simple_linear_regression(x, y):
    predictions = list()
    b0, b1 = coefficients(x, y)
    for row in x:
        yhat = b0 + b1 * row
        predictions.append(yhat)
    return predictions
