# Curation of non-planar axial distortions in serial section microscopy series

Non-planar axial distortions in serial section microscopy can occur at scales that are similar to the objects of interest within the data, which potentially impedes manual or automated processing. This library estimates a transform that compensates for non-planar axial distortions. We developed this library with a strong focus on focused ion beam scanning electron microscopy (FIB-SEM) acquisitions of *drosophila* nervous systems, but we expect our method to generalize well to other domains and imaging modalities. We submitted a manuscript for publication that describes our method in detail.

## Instructions
We implemented our method using the distributed computing framework [Apache Spark](http://spark.apache.org/) which allows for easy distribution onto compute clusters. If you do not have access to a compute cluster with Spark support, you can still run the estimation on your local machine, which -- in turn -- is limited by memory constraints. In the following, we will list the prerequisites and give instructions and examples for the execution of our method.

### Prerequisites

### Execution

### Examples
