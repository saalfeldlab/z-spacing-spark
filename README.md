# Curation of non-planar axial distortions in serial section microscopy series

Non-planar axial distortions in serial section microscopy can occur at scales that are similar to the objects of interest within the data, which potentially impedes manual or automated processing. This library estimates a transform that compensates for non-planar axial distortions. We developed this library with a strong focus on focused ion beam scanning electron microscopy (FIB-SEM) acquisitions of *drosophila* nervous systems, but we expect our method to generalize well to other domains and imaging modalities. We submitted a manuscript for publication that describes our method in detail.

## Instructions
We implemented our method using the distributed computing framework [Apache Spark](http://spark.apache.org/) which allows for easy distribution onto compute clusters. If you do not have access to a compute cluster with Spark support, you can still run the estimation on your local machine, which -- in turn -- is limited by memory constraints. In the following, we will list the prerequisites and give instructions and examples for the execution of our method.

### Prerequisites and build
This project uses maven for dependency management. Maven can resolve most depedendencies that are listed in `pom.xml` on its own. Therefore, manual library management is minimal.

- Compute cluster with Spark support, e.g. [Amazon Web Services](https://aws.amazon.com/elasticmapreduce/details/spark/) or your own cluster (if present). Alternatively, you can run the code on your local machine.
- [Apache maven 3](https://maven.apache.org/)
- [em-thickness-estimation](https://github.com/saalfeldlab/em-thickness-estimation)[1] in your local maven repository.

With all prerequisites fulfilled, go to the project root directory and run
```bash
mvn package
```
which will create a jar file located in the `target` directory. Copy this file to a location that is accessible from your cluster.

You might need to join the version and scala version of the spark-core dependency to adjust to your cluster settings before building the project. Also, if you want to run your code on your local machine, you should change the scope from provided to compile:
```xml
<dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-core_2.10</artifactId> <!-- change to scala version running on your cluster -->
    <version>1.5.1</version> <!-- change to spark version running on your cluster -->
    <scope>provided</scope> <!-- change to compile if run locally -->
</dependency>
```

### Execution

### Examples


## References
 - [1] Hanslovsky et al. "Post-acquisition image based compensation for thickness variation in microscopy section series", ISBI 2015
