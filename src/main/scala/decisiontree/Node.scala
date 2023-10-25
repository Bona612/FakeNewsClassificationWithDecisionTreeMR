package decisiontree

import decisiontree.DecisionTree

case class Node[A](label: A, left: DecisionTree[A], right: DecisionTree[A]) extends DecisionTree[A]