import numpy as np
nums = [1, 2, 3, 4, 5]
sqrd_nums = map(lambda x: x ** 2, nums)
#================================================================================================
# NumPy array broadcasting
nums = [-2,-1, 0, 1, 2]
nums ** 2
#=================================================================================================
# For loop (inefficient option)
sqrd_nums = []
for num in nums:
    sqrd_nums.append(num ** 2)

# list comp approach
sqrd_nums = [num**2 for num in nums]

#numpy array approach
nums_np = np.array(nums)
nums_np**2

# ================================== numpy Indexing ==============================================
nums2 = [ [1, 2, 3],
          [4, 5, 6] ]
nums2_np[0,1] # ==> 2
nums2_np[:,0] #==> [1,4]