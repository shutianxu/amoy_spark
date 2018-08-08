


seqOp = (lambda x, y: (x[0] + y, x[1] + 1))
combOp = (lambda x, y: (x[0] + y[0], x[1] + y[1]))
sc.parallelize([1, 2, 3, 4]).aggregate((0, 0), seqOp, combOp)
sc.parallelize([]).aggregate((0, 0), seqOp, combOp)




from pyspark import SparkFiles
path = os.path.join(tempdir, "test.txt")
with open(path, "w") as testFile:
	_ = testFile.write("100")
sc.addFile(path)
def func(iterator):
	with open(SparkFiles.get("test.txt")) as testFile:
		fileVal = int(testFile.readline())
		return [x * fileVal for x in iterator]
sc.parallelize([1, 2, 3, 4]).mapPartitions(func).collect()




x = sc.parallelize([("a", 1), ("b", 4)])
y = sc.parallelize([("a", 2)])
[(x, tuple(map(list, y))) for x, y in sorted(list(x.cogroup(y).collect()))]



m = sc.parallelize([(1, 2), (3, 4)]).collectAsMap()
m[1]
m[3]




#按照KEY值合并
x = sc.parallelize([("a", 1), ("b", 1), ("a", 2)])
def to_list(a):
    return [a]

def append(a, b):
    a.append(b)
    return a

def extend(a, b):
    a.extend(b)
    return a
sorted(x.combineByKey(to_list, append, extend).collect())




rdd = sc.parallelize(range(1000), 10)
rdd.countApprox(1000, 1.0)



sorted(sc.parallelize([1, 2, 1, 2, 2],).countByValue().items())



x = sc.parallelize([("a", ["x", "y", "z"]), ("b", ["p", "r"])])
def f(x): return x
x.flatMapValues(f).collect()


















