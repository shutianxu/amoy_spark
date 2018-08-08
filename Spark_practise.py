nums = sc.parallelize([1,2,3,4])
squared = nums.map(lambda x :x*x).collect()
for num in squared:
    print("%i" %(num))


lines = sc.parallelize(["hello world","hi"])
words = lines.flatMap(lambda line : line.split(" "))
words.first()


nums = sc.parallelize([1,2,3,3])
squared = nums.map(lambda x :x+1).collect()
print(squared)


#???
nums = sc.parallelize([1,2,3,3])
squared = nums.flatMap(lambda x :x.to(3)).collect()
print(squared)


nums = sc.parallelize([1,2,3,3])
squared = nums.filter(lambda x :x != 1).collect()
print(squared)


nums = sc.parallelize([1,2,3,3])
squared = nums.collect()
print(squared)


nums = sc.parallelize([1,2,3,3])
squared = nums.count()
print(squared)


nums = sc.parallelize([1,2,3,3])
squared = nums.countByValue()
print(squared)

#并集
num1 = sc.parallelize([1,2,3,3])
num2 = sc.parallelize([3,4,5])
squared = num1.union(num2).collect()
print(squared)

#求两个RDD共同的元素的RDD
num1 = sc.parallelize([1,2,3,3])
num2 = sc.parallelize([3,4,5])
squared = num1.intersection(num2).collect()
print(squared)

#移除一个RDD内容
num1 = sc.parallelize([1,2,3,3])
num2 = sc.parallelize([3,4,5])
squared = num1.subtract(num2).collect()
print(squared)

#做笛卡尔积
num1 = sc.parallelize([1,2,3,3])
num2 = sc.parallelize([3,4,5])
squared = num1.cartesian(num2).collect()
print(squared)




#reduce算聚合
nums = sc.parallelize([1,2,3,3])
squared = nums.reduce(lambda x,y:x + y)
print(squared)



nums = sc.parallelize([1,2,3,3])
squared = nums.fold(lambda x,y:x)
print(squared)


#????
sumCount = nums.aggregate((0,0),(lambda acc,value:(acc[0] + value ,acc[1] + 1),(lambda acc1,acc2 : (acc1[0] + acc2[0] , acc1[1] + acc2[1]))))
return sumCount[0]/float(sumCount[1])


#???
nums = sc.parallelize([1,2,3,3])
squared = nums.takeOrdered(2)(myOrdering)
print(squared)



#???
nums = sc.parallelize([1,2,3,3])
squared = nums.takeSample(false,1).collect()
print(squared)





