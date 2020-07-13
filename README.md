# enron-spark-analysis

Data exploration and analysis of a collection of approximately 500,000 emails exchanged by employees of the Enron corporation in the late 1990s and early 2000s.<br />
Data structure: Hadoop Sequence File (each binary object represents an email message)<br />
Framework used: Apache Spark<br />
<br />
Q1: Write a Python function extract_email_network() that takes an RDD as argument and returns an RDD of triples (S, R, T), each of which represents an email transmission from the sender S to the recipient R occurring at time T.<br />
<br />
Q2: Convert the email network extracted in Question 1 to a weighted network in which every two nodes are connected by at most two edges (one in either direction), and the weight of each edge (a, b) is the number of Email messages sent from a to b.<br />
<br />
Q3.1: Write a function get_out_degrees() that takes an RDD representing a weighted network as argument, and returns an RDD of pairs (d, n) satisfying the constraints below:<br />
• d is a non-negative integer<br />
• n is a string holding an email address<br />
• the weighted out-degree of n is d<br />
• there is exactly one pair (d, n) for each node n in the input network (even if its weighted out-degree is 0)<br />
• the output is sorted in the descending lexicographical order of the integer/string pairs<br />
<br />
Q3.2: Write a function get_in_degrees() that takes an RDD representing a weighted network as argument, and returns an RDD of pairs (d, n) satisfying the constraints below:<br />
• d is a non-negative integer<br />
• n is a string holding an email address<br />
• the weighted in-degree of n is d<br />
• there is exactly one pair (d, n) for each node n in the input network (even if its weighted in-degree is 0)<br />
• the output is sorted in the descending lexicographical order of the integer/string pairs<br />
<br />
Q4.1: Write a function get_out_degree_dist() that takes an RDD representing a weighted network, and returns an RDD of pairs mapping each weighted out-degree of a node in the network to the number of nodes having this out-degree. The output RDD must be sorted in the ascending order of the out-degrees.<br />
<br />
Q4.2: Write a function get_in_degree_dist() that takes an RDD representing a weighted network, and returns an RDD of pairs mapping each weighted in-degree of a node in the network to the number of nodes having this in-degree. The output RDD must be sorted in the ascending order of the in-degrees.<br />
