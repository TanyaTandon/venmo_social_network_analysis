
# Problem 1 
**Social Network Analysis**
The transaction file represents Venmo’s social network: the nodes of the network correspond to users,
and the edges represent the transactions between users. One of the fundamental properties of every
network is its degree distribution (https://en.wikipedia.org/wiki/Degree_distribution), i.e. the number of
edges/connections an individual has to other nodes in the network. There exist two types of network:
directed (e.g. Facebook) and undirected (e.g. Twitter: user A following user B does not imply that user B
follows user A); depending on the network type, the number of degrees of each node can vary. Venmo is
a directed network, as the act of user A sending money to user B has a direction associated with it. In
directed networks the exist two types of degrees: in-degree (i.e. number of incoming connections) and
out-degree (i.e. number of outgoing connections).
For this problem you have to perform the following three tasks:
1. Plot Venmo’s degree distribution by treating the network as undirected.
2. Plot Venmo’s in-degree and out-degree distributions.
3. What is the percentage of reciprocal transactions in the network (this should be a single
number)? Reciprocity is defined as follows: both user A and user B have sent money to each
other. Create a plot that shows the percentage of reciprocal transactions over time, with six
month increments between January 1st 2010 (start date) and June 1st 2016 (end date).

# Problem 2
**Emoji & Text Analysis**
One of the most fun aspects of using Venmo is picking emojis to describe your spending habits. Various
analyses have tried to identify the most popular emojis in Venmo:
https://bankinnovation.net/2016/09/what-do-people-use-venmo-for-check-the-emojis/.
In this exercise, you have to perform the following three tasks:
1. Find the top 10 most popular emojis on Venmo.
2. Find the top 5 most popular emojis on Venmo by weekday (similar to the link above).
3. Analyzing the content of all transaction messages in a consistent manner is a hard task; some
descriptions only contain text, others only emojis and so on. In order to facilitate the
classification of transactions into different categories (e.g. food, drinks, utilities), you should
cluster transaction messages using text-based attributes. Examples of attributes include, but are
not limited to, the number of characters in a message, presence of emojis in the message, etc. In
your write-up explain your reasoning behind the attributes you selected (you need at least 5
attributes). When deciding what attributes to use, keep in mind that the end goal is to improve
the classification of messages by applying a separate text classification algorithm to each cluster,
as messages within the same cluster share some basic structural similarities. You DO NOT have
to perform any text classification for this problem. Include a brief overview of your results in the
write-up – some sort of visualization is expected.
