# parquet 介绍

### 嵌套数据模型

- value : 字段值
- repetition level : 重复级别
- definition level : 定义级别

Parquet支持嵌套的数据模型，类似于Protocol Buffers，每一个数据模型的schema包含多个字段，每一个字段有三个属性：重复次数、数据类型和字段名。
重复次数可以是以下三种：required(有且仅只出现1次)，repeated(出现0次或多次)，optional(出现0次或1次)。每一个字段的数据类型可以分成两种：
group(复杂类型)和primitive(基本类型)。

在Schema中所有的基本类型字段都是叶子节点，在这个Schema中一共存在6个叶子节点，如果把这样的Schema转换成扁平式的关系模型，就可以理解为该表包
含六个列。Parquet中没有Map、Array这样的复杂数据结构，但是可以通过repeated和group组合来实现这样的需求。
　　
由于一条记录中某一列可能出现零次或者多次，需要标示出哪些列的值构成一条完整的记录，这是由Striping/Assembly算法实现的。它的原理是每一个记录
中的每一个成员值有三部分组成：Value、Repetition level和Definition level。value记录了该成员的原始值，可以根据特定类型的压缩算法进行压缩，
两个level值用于记录该值在整个记录中的位置。