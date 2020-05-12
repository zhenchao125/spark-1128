/**
 * Author atguigu
 * Date 2020/5/12 10:50
 */
object Test {
    def main(args: Array[String]): Unit = {
        val arr1 = List(30, 50, 70, 60, 10, 20)
        println(arr1.dropRight(1))
        println(arr1.init)
        println(arr1.slice(0, arr1.length - 1))
    }
}
