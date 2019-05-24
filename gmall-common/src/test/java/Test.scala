import scala.collection.mutable

/**
  * Author lzc
  * Date 2019/5/15 7:41 PM
  */
object Test {
    def main(args: Array[String]): Unit = {
        val ints: mutable.Buffer[Int] = foo(1,2,3,4,5)
        println(ints)
    }
    def foo(a: Int*)={
        println(a.getClass)
        a.toBuffer += 100
    }
}
