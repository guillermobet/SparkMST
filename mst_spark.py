def splitter(row):
    return row.split(",")

def mapper(row):
    return (int(row[2]), (int(row[0]), int(row[1])))

def reducer(row, forest):
    valid_edges = []
    for edge in row[1]:
        reach1 = set()
        reach2 = set()
        condition1 = not in_same_tree(edge[0], edge[1], forest)
        reach1.add(edge[0])
        reach1.add(edge[1])
        condition2 = not union_find(edge[0], edge[1], reach1, forest)
        reach2.add(edge[0])
        reach2.add(edge[1])
        condition3 = not union_find(edge[1], edge[0], reach2, forest)
        if condition1 and condition2 and condition3:
            valid_edges += [edge]
    return valid_edges

def union_find(source, destination, reachable, forest):
    in_same_set = False
    if source not in forest:
        forest[source] = reachable
    else:
        temp_set = set()
        temp_set = temp_set.union(forest[source])
        for elem in temp_set:
            if destination in forest[source]:
                in_same_set = True
                break
        for elem in temp_set:
            if elem not in forest:
                forest[elem] = reachable
            forest[elem] = forest[elem].union(reachable)
    return in_same_set

def in_same_tree(source, destination, forest):
    if source in forest and destination in forest:
        if destination in forest[source] and source in forest[destination]:
            return True
        else:
            return False
    else:
        return False

#if __name__ == '__main__':
forest = {}
dataset = sc.textFile("./datasets/test2.csv")
is_min = True
result = (dataset.map(splitter)
                .map(mapper)
                .groupByKey()
                .sortByKey(ascending=is_min)
                .map(lambda x: (x[0], list(x[1]))))
valid_edges = []
total_weight = 0

for elem in result.toLocalIterator():
    weight = elem[0]
    for edge in elem[1]:
        reach1 = set()
        reach2 = set()
        condition1 = not in_same_tree(edge[0], edge[1], forest)
        reach1.add(edge[0])
        reach1.add(edge[1]) 
        condition2 = not union_find(edge[0], edge[1], reach1, forest)
        reach2.add(edge[0])
        reach2.add(edge[1])
        condition3 = not union_find(edge[1], edge[0], reach2, forest)
        if condition1 and condition2 and condition3:
            valid_edges += [edge]
            total_weight += weight

print valid_edges
print total_weight