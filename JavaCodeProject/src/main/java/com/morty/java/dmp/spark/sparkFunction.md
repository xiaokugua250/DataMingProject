##spark operations

###spark Transformations
- reduceByKey(function) combines values with the same key using the provided function.
- groupByKey() maps unique keys to an array of values assigned to that key.
- combineByKey() combines values with the same key, but uses a different result type.
- mapValues(function) applies a function to each of the values in the RDD.
- flatMapValues(function) applies a function to all values, but in this case the function returns an iterator to each newly generated value.
Spark then creates new pairs for each value mapping the original key to each of the generated values.
- keys() returns an RDD that contains only the keys from the pairs.
- values() returns an RDD that contains only the values from the pairs.
- sortByKey() sorts the RDD by the key value.
- subtractByKey(another RDD) removes all element from the RDD for which there is a key in the other RDD.
- join(another RDD) performs an inner join between the two RDDs; the result will only contain keys present in both and those keys will be mapped to all values in both RDDs.
- rightOuterJoin(another RDD) performs a right outer join between the two RDDs in which all keys must be present in the other RDD.
- leftOuterJoin(another RDD) performs a left outer join between the two RDDs in which all of the keys must be present in the original RDD.
- cogroup(another RDD) groups data from both RDDs that have the same key.

### common actions in spark
- collect() returns all elements in the RDD.
- count() returns the number of elements in the RDD.
- countByValue() returns the number of elements with the specified value that are in the RDD.
- take(count) returns the requested number of elements from the RDD.
- top(count) returns the top "count" number of elements from the RDD.
- takeOrdered(count)(ordering) returns the specified number of elements ordered by the specified ordering function.
- takeSample() returns a random sample of the number of requested elements from the RDD.
- reduce() executes the provided function to combine the elements into a result set.
- fold() is similar to reduce(), but provides a "zero value."
- aggregate() is similar to fold() (it also accepts a zero value), but is used to return a different type that the source RDD.
- foreach() executes the provided function on each element in the RDD, which good for things like writing to a database or publishing to a web service.

### data analysis with spark
The steps for analyzing data with Spark can be grossly summarized as follows:

1. Obtain a reference to an RDD.
2. Perform transformations to convert the RDD to the form you want to analyze.
3. Execute actions to derive your result set.
An important note is that while you may specify transformations, they do not actually get executed until you specify an action.
This allows Spark to optimize transformations and reduce the amount of redundant work that it needs to do. Another important thing to note is that once an action is executed, you'll need to apply the transformations again in order to execute more actions.
If you know that you're going to execute multiple actions then you can persist the RDD before executing the first action by invoking the persist() method; just be sure to release it by invoking unpersist() when you're done.

### Spark in a distributed environment
Spark consists of two main components:

- Spark Driver
- Executors
The Spark Driver is the process that contains your main() method and defines what the Spark application should do.
 This includes creating RDDs, transforming RDDs, and applying actions. Under the hood, when the Spark Driver runs, it performs two key activities:

1. **Converts your program into tasks:** Your application will contain zero or more transformations and actions, so it's the Spark Driver's responsibility to convert those into executable tasks that can be distributed across the cluster to executors.
Additionally, the Spark Driver optimizes your transformations into a pipeline to reduce the number of actual transformations needed and builds an execution plan. It is that execution plan that defines how tasks will be executed and the tasks themselves are bundled up and sent to executors.
2. **Schedules tasks for executors:** From the execution plan, the Spark Driver coordinates the scheduling of each task execution. As executors start, they register themselves with the Spark Driver, which gives the driver insight into all available executors.
 Because tasks execute against data, the Spark Driver will find the executors running on the machines with the correct data, send the tasks to execute, and receive the results.

Spark Executors are processes running on distributed machines that execute Spark tasks. Executors start when the application starts and typically run for the duration of the application. They provide two key roles:

1. Execute tasks sent to them by the driver and return the results.
2. Maintain memory storage for hosting and caching RDDs.

The cluster manager is the glue that wires together drivers and executors. Spark provides support for different cluster managers, including Hadoop YARN and Apache Mesos. The cluster manager is the component that deploys and launches executors when the driver starts. You configure the cluster manager in your Spark Context configuration.

The following steps summarize the execution model for distributed Spark applications:

1. Submit a Spark application using the spark-submit command.
2. The spark-submit command will launch your driver program's main() method.
3. The driver program will connect to the cluster manager and request resources on which to launch the executors.
4. The cluster manager deploys the executor code and starts their processes on those resources.
5. The driver runs and sends tasks to the executors.
6. The executors run the tasks and sends the results back to the driver.
7. The driver completes its execution, stops the Spark context, and the resources managed by the cluster manager are released.
