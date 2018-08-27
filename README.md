# A Distributed Rough Set Theory based Algorithm for an Efficient Big Data Pre-processing under the Spark Framework

Big Data reduction is a main point of interest across a wide variety of fields. This domain was further investigated when the difficulty in quickly acquiring the most useful information from the huge amount of data at hand was encountered. To achieve the task of data reduction, specifically feature selection, several state-of-the-art methods were proposed. However, most of them require additional information about the given data for thresholding, noise levels to be specified or they even need a feature ranking procedure. Thus, it seems necessary to think about a more adequate feature selection technique which can extract features using information contained within the dataset
alone. Rough Set Theory (RST) can be used as such a technique to discover data dependencies and to reduce the number of features contained in a dataset using the data alone, requiring no additional information. However, despite being a powerful feature selection technique, RST is computationally expensive and only practical for small datasets. Therefore, we present a novel efficient distributed Rough Set Theory based algorithm for large-scale data pre-processing under the Spark framework. Our method named Sp-RST reduces the computational effort of the rough set computations without any significant information loss, as it splits the given dataset into partitions with smaller numbers of features which are then processed in parallel. This distributed RST version is part of the Marie Sklodowska-Curie [RoSTBiDFramework](http://rostbid.dcs.aber.ac.uk/) project. 

## Getting Started

Please, follow the following instructions to use Sp-RST.

### Installing

To be able to use the Sp-RST code, you will need to install the following:

1. Install Scala 2.11.8
2. Install Spark 2.1.1

(please, see build.sbt for the configuration)

### Main Sp-RST Parameters

- val rawdata = Path to the input data set
- val sep = Separator type
- val savingPath = Path where results will be saved
- val nbColumn = Number of partitions 
- val nbIterIfPerFeat = Number of iterations

### Running Sp-RST

To run the code, you need to go through the below steps:

1. Create the .jar file (using sbt)
2. To execute Sp-RST locally:
- 
-

## Publications

## ACKNOWLEDGMENT
This work is part of a project that has received funding from the European Union’s Horizon 2020 research and innovation programme under the Marie Skłodowska-Curie grant agreement No 702527.
