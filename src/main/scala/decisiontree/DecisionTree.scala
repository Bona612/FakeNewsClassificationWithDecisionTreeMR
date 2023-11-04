package decisiontree

import org.apache.spark.sql.functions.{lit, monotonically_increasing_id, row_number}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.expressions.Window

import java.io._
import scala.io.Source

sealed trait DecisionTree {
  def printToFile(filename: String): String
  def printToFile(filename: String, rule: String): String
  def getParent(): Option[DecisionTree]

  def predict(input: DataFrame,decisionTree: DecisionTree): Array[Int]
}

case class Leaf(label: String,parent: Option[DecisionTree]) extends DecisionTree {
  def getLabel(): String = { this.label }

  def printToFile(filename: String, rule: String): String = {
    println("FOGLIA")
    var addedRule: String = rule + ", " + label.toString + "\n"
    println("RULE: " + addedRule)

    addedRule
  }

  // Inutile
  override def printToFile(filename: String): String = {
    val file = new File(filename)
    val writer = new PrintWriter(file)

    var rule: String = label.toString

   rule
  }

  override def getParent(): Option[DecisionTree] = {parent}

  override def predict(input: DataFrame,decisionTree: DecisionTree): Array[Int] = { return null}
}

case class Node(var attribute: String, value: Double, var left: DecisionTree, var right: DecisionTree, parent: Option[Node]) extends DecisionTree {

  def writeRulesToFile(filename: String): Unit = {
    val file = new File(filename)
    val writer = new PrintWriter(file)
    val rule = printToFile(filename)
    try {
      writer.write(rule + "\n")
    } finally {
      writer.close()
    }
  }

  def getAttribute(): String = {
    this.attribute
  }

  def setAttribute(): Unit = {
    this.attribute = "Cambiato"
  }

  def getValue(): Double = {
    this.value
  }

  def getLeftChild(): Option[DecisionTree] = {
    var tmp: Option[DecisionTree] = None
    if (this.left != null) {
      tmp = Option(this.left)
    }
    tmp
  }

  def getRightChild(): Option[DecisionTree] = {
    var tmp: Option[DecisionTree] = None
    if (this.right != null) {
      tmp = Option(this.right)
    }
    tmp
  }

  def insertLeftChild(node: DecisionTree): Unit = {
    this.left = node
    return Unit
  }

  def insertRightChild(node: DecisionTree): Unit = {
    this.right = node
    return Unit
  }


  override def printToFile(filename: String, rule: String): String = {
    var addedRule = rule + ", "

    addedRule = addedRule + attribute.toString
    println("RULE: " + addedRule)
    val leftRule: String = left.printToFile(filename, addedRule + " < " + value.toString)
    val rightRule: String = right.printToFile(filename, addedRule + " >= " + value.toString)
    val ret: String = leftRule + rightRule
    ret
  }

  def printToFile(filename: String): String = {
    var rule: String = ""

    println("INIZIO")
    rule = rule + attribute.toString
    println("RULE: " + rule)
    val leftRule: String = left.printToFile(filename, rule + " < " + value.toString)
    val rightRule: String = right.printToFile(filename, rule + " >= " + value.toString)
    val ret: String = leftRule + rightRule
    ret
  }

  override def getParent(): Option[DecisionTree] = {
    parent
  }

  override def predict(input: DataFrame, decisionTree: DecisionTree): Array[Int] = {
    var currentNode = Option(decisionTree)
    // Definisci una finestra per l'ordinamento
    val windowSpec = Window.orderBy(lit(1))

    // Aggiungi una colonna con l'indice a partire da 0
    val dfWithIndex = input.withColumn("index", row_number().over(windowSpec) - 1)
    println("dfWithIndex.show()")
    dfWithIndex.show()
    val res: Array[Int] = new Array[Int](dfWithIndex.count().toInt)
    println("DENTRO PREDICT")

    dfWithIndex.collect().foreach{ row: Row =>
      var stop: Boolean = true

      while(stop) {
        currentNode match {
          case None => {}
          case Some(value) => {
            if (value.isInstanceOf[Leaf]) {
              val index = row.getAs[Int]("index")
              res(index) = value.asInstanceOf[Leaf].getLabel().toInt
              println(res(index))
              stop = false
              println("RES")
              println(res.mkString("Array(", ", ", ")"))
            }
            else {
              if (value.asInstanceOf[Node].getValue() > row.getAs[Double](value.asInstanceOf[Node].getAttribute())) {
                value.asInstanceOf[Node].getLeftChild() match {
                  case None => {}
                  case Some(value) => {
                    if (value.isInstanceOf[Leaf]) {
                      currentNode = Option(value.asInstanceOf[Leaf])
                    }
                    else currentNode = Option(value.asInstanceOf[Node])
                  }
                }
              }
              else {
                value.asInstanceOf[Node].getRightChild() match {
                  case None => {}
                  case Some(value) => {
                    if (value.isInstanceOf[Leaf]) {
                      currentNode = Option(value.asInstanceOf[Leaf])
                    }
                    else currentNode = Option(value.asInstanceOf[Node])
                  }
                }
              }
            }
          }
        }
      }
    }
    println(res.mkString("Array(", ", ", ")"))
    res
  }
}

  object DecisionTree {
    def fromFile(filename: String): DecisionTree = {
      // Open the file for reading
      val file = Source.fromFile(filename)

      var nodes: Array[String] = null
      var values: Array[String] = null
      var tree: Node = null
      var current: Option[DecisionTree] = null
      var attribute: String = null
      var value: Double = 0.0
      var operation: String = null
      var last: String = null
      var parent: Option[DecisionTree] = null
      var current_operation: String = null

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
            current_operation = values(1)
            value = values(2).toDouble

            if (tree == null) {
              println("CURRENT == NULL")
              operation = current_operation
              tree = Node(attribute, value, null, null, None)
              current = Option(tree)
            }
            else if (current.isEmpty) {
              println("INSERISCO MANCANTE ")
              println(attribute + " " + value.toString)
              if (operation == "<") {
                parent.get.asInstanceOf[Node].insertLeftChild(Node(attribute, value, null, null, Option(parent.get.asInstanceOf[Node])))
                current = parent.get.asInstanceOf[Node].getLeftChild()
                println("INSERISCO MANCANTE LEFT ")
                println(parent.get.asInstanceOf[Node].getAttribute() + " " + parent.get.asInstanceOf[Node].getValue())
                println(current.get.asInstanceOf[Node].getAttribute() + " " + current.get.asInstanceOf[Node].getValue())
              }
              else {
                parent.get.asInstanceOf[Node].insertRightChild(Node(attribute, value, null, null, Option(parent.get.asInstanceOf[Node])))
                current = parent.get.asInstanceOf[Node].getRightChild()
                println("INSERISCO MANCANTE RIGHT ")
                println(parent.get.asInstanceOf[Node].getAttribute() + " " + parent.get.asInstanceOf[Node].getValue())
                println(current.get.asInstanceOf[Node].getAttribute() + " " + current.get.asInstanceOf[Node].getValue())
              }
              /* if(operation == "<"){
              current = current.get.asInstanceOf[Node].getLeftChild()
            }
            else{
              current = current.get.asInstanceOf[Node].getRightChild()
            }
            */
              parent = current.get.asInstanceOf[Node].getParent()
            }
            else {
              // FORSE SERVE UN CONTROLLO SUL FATTO CHE POTREBBE ESSERE UNA FOGLIA
              // MA NELLA REALTà LA FOGLIA LA CAVO PRIMA
              val current_node: Node = current.get.asInstanceOf[Node]
              parent = current_node.parent

              if (current_node.getAttribute() != attribute || current_node.getValue() != value) {
                println("DIVERSO")
                if (operation == "<") {
                  current_node.insertLeftChild(Node(attribute, value, null, null, Option(current_node)))
                  current = current.get.asInstanceOf[Node].getLeftChild()
                  println("INSERISCO SINISTRA " + current_node.getAttribute() + " " + current_node.getValue().toString)
                  println(attribute + " " + value.toString)
                }
                else {
                  current_node.insertRightChild(Node(attribute, value, null, null, Option(current_node)))
                  current = current.get.asInstanceOf[Node].getRightChild()
                  println("INSERISCO DESTRA " + current_node.getAttribute() + " " + current_node.getValue().toString)
                  println(attribute + " " + value.toString)
                }
              }
              else if (current_node.getAttribute() == attribute && current_node.getValue() == value) {
                println("UGUALE")
                if (current_operation == "<") {
                  println("SCORRIMENTO SINISTRA " + current_node.getAttribute() + " " + current_node.getValue().toString)
                  println(current.get.asInstanceOf[Node].getAttribute() + " " + current.get.asInstanceOf[Node].getValue().toString)
                  current = current.get.asInstanceOf[Node].getLeftChild()
                }
                else {
                  println("SCORRIMENTO DESTRA" + current_node.getAttribute() + " " + current_node.getValue().toString)
                  println(current.get.asInstanceOf[Node].getAttribute() + " " + current.get.asInstanceOf[Node].getValue().toString)
                  current = current.get.asInstanceOf[Node].getRightChild()
                }
                parent = Option(current_node) //current.get.asInstanceOf[Node].getParent()
                println(parent.isEmpty)
              }
            }
            operation = current_operation
          }


          if (current.isEmpty) {
            val leaf: Leaf = Leaf(last, parent)
            println("INSERISCO FOGLIA MANCANTE ")
            println(attribute + " " + value.toString)
            if (current_operation == "<") {
              parent.get.asInstanceOf[Node].insertLeftChild(leaf)
              println("INSERISCO FOGLIA MANCANTE LEFT ")
              println(leaf.getLabel())
            }
            else {
              parent.get.asInstanceOf[Node].insertRightChild(leaf)
              println("INSERISCO FOGLIA MANCANTE RIGHT ")
              println(leaf.getLabel())
            }
          }
          else {
            val leaf: Leaf = Leaf(last, current)
            if (current_operation == "<") {
              current.get.asInstanceOf[Node].insertLeftChild(leaf)
              println("LABEL LEFT")
              println(leaf.getLabel())
            }
            else {
              current.get.asInstanceOf[Node].insertRightChild(leaf)
              println("LABEL RIGHT")
              println(leaf.getLabel())
            }
          }
          current = Option(tree)
          parent = current.get.asInstanceOf[Node].parent
        }
        tree
      }
      finally {
        file.close() // Close the file when done
      }
    }
  }