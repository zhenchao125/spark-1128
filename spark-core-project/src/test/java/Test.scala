import java.text.DecimalFormat

/**
 * Author atguigu
 * Date 2020/5/12 10:50
 */
object Test {
    def main(args: Array[String]): Unit = {
        /*val arr1 = List(30, 50, 70, 60, 10, 20)
        println(arr1.dropRight(1))
        println(arr1.init)
        println(arr1.slice(0, arr1.length - 1))*/
        //
        val f = new DecimalFormat("0000.00%")
        println(f.format(1.23445))
        println(f.format(12345.248))
        println(f.format(0.34568))
    }
}
