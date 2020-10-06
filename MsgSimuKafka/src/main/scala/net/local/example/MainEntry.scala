package net.local.example

object MainEntry {
  def main(args: Array[String]): Unit = {
    val parser = new ArgumentsParser()
    if (!parser.parseArgs(args)) {
      return
    }
    val mode = parser.mode
    mode match {
      case 1 => FIXGenerator.FIXGenerate(
        parser.kafkabrokers,
        parser.kafkatopic)
      case 2 => KuduFixDataStreamer.kuduFixDataStreamWriter(
        parser.kafkabrokers,
        parser.kafkatopic,
        parser.kuduMaster,
        parser.tableName,
        parser.uselocal)
      case 3 => CreateFixTable.createFixTable(
        parser.kuduMaster,
        parser.tableName,
        parser.hashpartitions,
        parser.numberofdays,
        parser.tablereplicas)
    }
  }
}
