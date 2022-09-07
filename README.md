# genD

genD is a tool used for generating rows with special requirement about the column's null values and distinct limitation. you should build your table with DDL before running genD.

genD receive your column type(only int and varchar row) and null value percentage and distinct percentage as parameters.

```
./genD --P="4329" --colNum=2 --typeBits=0 --rows="200000 200000" --nullP="1 1 1 1" --distinctP="100 100 100 100"
```

`-P` means the mysql port

`--colNum` means the number of naaj column

`--typeBits` means the naaj-col type, left side and right side are the same. for example 3 means bits="00...011", 1 represent the varchar and 0 means the int type. so 3 means the na-join columns(only two here) is both varchar type.

`--rows` means the left table size and the right table size.

`--nullP` means the null values percentage of total rows. the default 1 means the 1% of total rows. you can specify 2*colNum times for left side and right side.

`--distinctP` means the distinct values percenetage of total rows. the default value 100 means no limiatation here, we enumerate value in a full scope, but duplication is unavlidable.
