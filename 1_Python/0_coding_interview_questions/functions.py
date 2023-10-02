# Using Positional Arguments
def multiply(*args):
    result = 1
    for arg in args:
        result = result * arg
    return result

print(multiply(1,2,3,4))
nums = (2, 3, 4, 5)
multiply(*nums)


# Can alos be done as
from functools import reduce

def multiply(*args):
    return reduce(lambda x, y: x * y, args, 1)

'''----------------------------------------------    KEY WORD ARGUMENT ----------------------------------------------'''
# **kwargs
def func_with_kwargs(arg1=1, arg2=2):
    pass

def multiply_kwargs(**kwargs):
    result = 1
    for (key, value) in kwargs.items():
        print(key + ' = ' + str(value))
        result = result * value
    return result
multiply_kwargs(num1=1, num2=2, num3=3, num4=4)

# Another use of double asterisk **
nums = {'num1':10,'num2':20,'num3':30}
multiply(**nums)

'''------------------------------------------------------- LAMBDA -------------------------------------------------------'''
