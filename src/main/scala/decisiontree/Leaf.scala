package decisiontree

import decisiontree.DecisionTree

case class Leaf[A](label: A) extends DecisionTree[A]
