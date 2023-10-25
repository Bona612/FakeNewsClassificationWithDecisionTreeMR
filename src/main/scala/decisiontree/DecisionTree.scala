package decisiontree

import java.io._
import scala.io.Source

sealed trait DecisionTree {
  def printToFile(filename: String): Unit
  def printToFile(filename: String, rule: String): Unit
}

case class Leaf(label: String) extends DecisionTree {
  def printToFile(filename: String, rule: String): Unit = {
    val file = new File(filename)
    val writer = new PrintWriter(file)

    var addedRule = rule + ", " + label.toString

    try {
      writer.write(addedRule + "\n")
    } finally {
      writer.close()
    }
  }

  // Inutile
  override def printToFile(filename: String): Unit = {
    val file = new File(filename)
    val writer = new PrintWriter(file)

    var rule: String = label.toString

    try {
      writer.write(rule + "\n")
    } finally {
      writer.close()
    }
  }
}

case class Node(attribute: String, value: Double, var left: DecisionTree, var right: DecisionTree, parent: Option[Node]) extends DecisionTree {

  def getAttribute(): String = {
    this.attribute
  }

  def getValue(): Double = {
    this.value
  }

  def getLeftChild(): DecisionTree = {
    this.left
  }

  def getRightChild(): DecisionTree = {
    this.right
  }

  def insertLeftChild(node: DecisionTree): Unit = {
    this.left = node
    return Unit
  }

  def insertRightChild(node: DecisionTree): Unit = {
    this.right = node
    return Unit
  }


  override def printToFile(filename: String, rule: String): Unit = {
    var addedRule = rule + ", "

    addedRule = addedRule + attribute.toString
    left.printToFile(filename, addedRule + " < " + value.toString)
    right.printToFile(filename, addedRule + " >= " + value.toString)
  }

  def printToFile(filename: String): Unit = {
    var rule: String = ""

    rule = rule + attribute.toString
    left.printToFile(filename, rule + " < " + value.toString)
    right.printToFile(filename, rule + " >= " + value.toString)
  }
}
object DecisionTree{
  def fromFile(filename: String): DecisionTree = {
    // Open the file for reading
    val file = Source.fromFile(filename)

    var nodes: Array[String] = null
    var values: Array[String] = null
    var tree: Node = null
    var current: Option[DecisionTree] = null
    var attribute: String = null
    var value: Double = null
    var operation: String = null
    var last: String = null

    try {
      // Iterate over the lines in the file
      for (line <- file.getLines()) {
        println(line) // Process the line (in this case, printing it)
        nodes = line.split(",")

        // Remove the last element and get it
        last = nodes.last.trim()
        var middleNodes = nodes.dropRight(1)

        for (node <- middleNodes) {
          values = node.trim().split(" ")
          attribute = values(0)
          val current_operation: String = values(1)
          value = values(2).toDouble

          if (current == null) {
            operation = current_operation
            tree = Node(attribute, value, null, null, None)
            current = Option(tree)
          }
          else {
            // FORSE SERVE UN CONTROLLO SUL FATTO CHE POTREBBE ESSERE UNA FOGLIA
            // MA NELLA REALTà LA FOGLIA LA CAVO PRIMA
            val current_node: Node = current.get.asInstanceOf[Node]

            if (current_node.getAttribute() != attribute || current_node.getValue() != value) {
              if (operation == "<") {
                current_node.insertLeftChild(Node(attribute, value, null, null, Option(current_node)))
                current = Option(current_node.getLeftChild())
              }
              else {
                current_node.insertRightChild(Node(attribute, value, null, null, Option(current_node)))
                current = Option(current_node.getRightChild())
              }
            }
            else if (current_node.getAttribute() == attribute && current_node.getValue() == value) {
              if (current_operation != operation) {
                // INSERIRE FIGLIO MANCANTE
                if (operation == "<") {
                  current_node.insertLeftChild(Node(attribute, value, null, null, Option(current_node)))
                  current = Option(current_node.getLeftChild())
                }
                else {
                  current_node.insertRightChild(Node(attribute, value, null, null, Option(current_node)))
                  current = Option(current_node.getRightChild())
                }
              }
              // SCORRIMENTO DEL NODO DELL'ALBERO GIÀ INSERITO
              else {
                if (operation == "<") {
                  current = Option(current_node.getLeftChild())
                }
                else {
                  current = Option(current_node.getRightChild())
                }
              }
            }
          }
        }
        val leaf: Leaf = Leaf(last)
        if (operation == "<") {
          current.get.asInstanceOf[Node].insertLeftChild(leaf)
        }
        else {
          current.get.asInstanceOf[Node].insertRightChild(leaf)
        }
        current = Option(tree)
      }
      tree
    }
    finally {
      file.close() // Close the file when done
    }
  }
}