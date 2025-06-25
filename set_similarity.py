from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from math import ceil
import sys


def get_prefix_length(length, threshold):
    return int(length - ceil(length * threshold) + 1)

def calculate_similarity(pair):
    set1 = set(pair[1][0][1])
    set2 = set(pair[1][1][1])
    intersection = len(set1.intersection(set2))
    union = len(set1.union(set2))
    return intersection / union >= threshold

def parse_line(line):
    fields = line.split(',')
    year = line.split(',')[3].split(' ')[0].split('/')[2]
    return ((int(fields[0]),int(year)), fields[1])

def get_prefix(transaction_data, order_list, threshold):
    sorted_items = sorted(transaction_data, key=lambda x: order_list.index(x))
    prefix_length = get_prefix_length(len(sorted_items), threshold)
    return (sorted_items[:prefix_length])

class SetSimilarity:
    def get_ordered_transactions(self, transactions_rdd, reduced_frequencies_bcast):
        freq_items = reduced_frequencies_bcast.value
        freq_dict = {item: rank for rank, item in enumerate(freq_items)}

        def order_transaction(transaction):
            trans_id, items = transaction
            ordered_items = sorted(list(items), key=lambda item: freq_dict.get(item, float('inf')))
            return trans_id, ordered_items

        ordered_transactions_rdd = transactions_rdd.map(order_transaction)
        return ordered_transactions_rdd

    def get_prefixes(self, ordered_transactions_rdd):
        def get_prefix(transaction):
            trans_id, items = transaction
            prefix_length = get_prefix_length(len(items), self.threshold)
            prefix = items[:prefix_length]
            return trans_id, (set(prefix),items)

        prefixes_rdd = ordered_transactions_rdd.map(get_prefix)
        return prefixes_rdd

    def calculate_similarity(self, sets, threshold):
        similarities = []
        for i in range(len(sets)):
            for j in range(i + 1, len(sets)):
                (trans_id1, items_str1, year1) = sets[i]
                # print(trans_id1, items_str1, year1)
                (trans_id2, items_str2, year2) = sets[j]

                set1 = set(items_str1.split(' '))
                set2 = set(items_str2.split(' '))

                if year1 != year2:
                    intersection = len(set1.intersection(set2))
                    union = len(set1.union(set2))
                    similarity = intersection / union

                    if similarity >= threshold:
                        key = (trans_id1,trans_id2)
                        similarities.append((key, similarity))
        return similarities


    def run(self, inputpath, outputpath, threshold):
        conf = SparkConf().setAppName("SetSimilarity")
        sc = SparkContext(conf=conf)
        
        self.threshold = threshold
        lines = sc.textFile(inputpath)
        elements = lines.map(lambda x: x.split(',')[1]).flatMap(lambda x: [(x, 1)])
        token_frequencies = elements.reduceByKey(lambda x, y: x + y)
        swapped_token_frequencies = token_frequencies.map(lambda x: (x[1], x[0]))
        # print(swapped_token_frequencies.collect())
        reduced_frequencies = swapped_token_frequencies.sortBy(lambda x: (x[0], x[1])).map(lambda x: x[1])
        # print(reduced_frequencies.collect())
        reduced_frequencies_bcast = sc.broadcast(reduced_frequencies.collect())
        # sorted_frequencies = reduced_frequencies.sortByKey(ascending=True)
        # print(sorted_frequencies.collect())
        # print(token_frequencies)
        # broadcast_item_freq = sc.broadcast(token_frequencies)
        transactions = lines.map(parse_line).groupByKey().mapValues(set)
        # print(transactions.collect())
        ordered_transactions = self.get_ordered_transactions(transactions, reduced_frequencies_bcast)
        # print(f'hi: {ordered_transactions.collect()}')
        prefixes_rdd = self.get_prefixes(ordered_transactions)


        def map_to_prefix_as_key(prefix_transaction):
            (trans_id, year), (prefix_set, all_item) = prefix_transaction
            for prefix in prefix_set:
                yield (prefix, str(trans_id) + ','+ str(year)+','+' '.join(all_item))


        # print(f'hello: {prefixes_rdd.collect()}')
        prefix_keyed_rdd = prefixes_rdd.flatMap(map_to_prefix_as_key)
        # print(prefix_keyed_rdd.collect())

        last_grouped_rdd = prefix_keyed_rdd.groupByKey().mapValues(list)
        # print(last_grouped_rdd.collect())
        similarity_rdd = last_grouped_rdd.flatMap(
            lambda group: self.calculate_similarity(
                [(int(each_set.split(',')[0]), each_set.split(',')[2], int(each_set.split(',')[1])) for each_set in group[1]],
                self.threshold
            )
        )
        # print(similarity_rdd.collect())
        standardized_similarity_rdd = similarity_rdd.map(
            lambda x: ((min(x[0]), max(x[0])), x[1])
        ).reduceByKey(lambda a, b: a)

        # print(standardized_similarity_rdd.collect())
        sorted_similarity_rdd = standardized_similarity_rdd.sortByKey()
        # print(sorted_similarity_rdd.collect())
        sorted_similarity_rdd.map(lambda x: f"({x[0][0]},{x[0][1]}):{x[1]}").saveAsTextFile(outputpath)
        # prefix_transaction = transactions.mapValues(lambda x: get_prefix(x, reduced_frequencies_bcast, self.threshold))
        # print(prefix_transaction.collect())

            # print(token_frequencies[i])



if __name__ == '__main__':
    threshold = float(sys.argv[3])  # Convert string argument to float for the threshold
    SetSimilarity().run(sys.argv[1], sys.argv[2], threshold)

