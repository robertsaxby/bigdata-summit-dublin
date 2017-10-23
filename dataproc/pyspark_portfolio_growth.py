import random
import time
from operator import add
from pyspark import SparkContext

sc = SparkContext()

def grow(seed):
    """
    Generate a random return on the investment, as a percentage, every year for the duration of a specified term.
    The function increases the value of the portfolio by the growth amount, which could be positive or negative,
    and adds a yearly sum that represents further investment.
    """
    random.seed(seed)
    portfolio_value = INVESTMENT_INIT
    for i in range(TERM):
        growth = random.normalvariate(MKT_AVG_RETURN, MKT_STD_DEV)
        portfolio_value += portfolio_value * growth + INVESTMENT_ANN
    return portfolio_value

INVESTMENT_INIT = 100000  # starting amount
INVESTMENT_ANN = 10000  # yearly new investment
TERM = 30  # number of years
MKT_STD_DEV = 0.18  # standard deviation


def simulation_run():
    """
    Parallelize: RDD containing seeds that are based on the current system time
    Then reduce to aggregate the values in the RDD
    """
    result = sc.parallelize([time.time() + i for i in xrange(100000)]) \
        .map(grow).reduce(add)/100000.

    print "\nReturn on investment: {}".format(result)


MKT_AVG_RETURN = 0.11 # percentage
simulation_run()
