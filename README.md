# Word Trends and Common Phrases Analysis Tool

## Project Description

For this project, I made two applications for analyzing datasets obtained from
http://snap.stanford.edu/data/memetracker9.html

## Technologies Used

* JDK - version 1.8.0_282
* Scala - version 2.13.4
* Hadoop - version 3.2.1

## Features

* Plots the number of occurrences for a word per day over time
* Lists the most common phrases with x-number of words
* Efficient analysis by implementing Hadoop MapReduce

To-do list:
* Add more thorough testing
* Add more thorough source code documentation

## Getting Started
   
> git clone git@github.com:revature-scalawags/PepinA97-Project1.git 

Navigate to the directory which you wish to compile and run:
> sbt compile

## Usage

For the Word Trender
> sbt --error run [WORD]

For the Common Phrases
> sbt --error run [NUM WORDS PER PHRASE] [NUM MINIMUM INSTANCES]