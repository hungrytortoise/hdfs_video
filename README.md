# hdfs_video
采用的是 springmvc框架，其他的亦可替换

##how to use?
- 将tools中的contants类中的namenode的ip 端口指定
- 将html中video标签src中的路径改成hdfs上的路径


##也可以替换其他的jq播放器的插件只要将video中的连接替换成需要的即可

<font color ='red'>特别注意，添加hadoop依赖的时候要用下面的方式，不然当运行的时候访问jsp页面 会抛出500的错误，搞了半天</font>

        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-hdfs</artifactId>
            <version>2.6.0</version>
        </dependency>
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-common</artifactId>
            <version>2.6.0</version>
            <exclusions>
                <exclusion>
                    <groupId>org.mortbay.jetty</groupId>
                    <artifactId>jetty</artifactId>
                </exclusion>
                <exclusion>
                    <groupId>org.mortbay.jetty</groupId>
                    <artifactId>jetty-util</artifactId>
                </exclusion>
                <exclusion>
                    <groupId>org.mortbay.jetty</groupId>
                    <artifactId>jsp-2.1</artifactId>
                </exclusion>
                <exclusion>
                    <groupId>org.mortbay.jetty</groupId>
                    <artifactId>jsp-api-2.1</artifactId>
                </exclusion>
                <exclusion>
                    <groupId>org.mortbay.jetty</groupId>
                    <artifactId>servlet-api-2.1</artifactId>
                </exclusion>
                <exclusion>
                    <groupId>javax.servlet</groupId>
                    <artifactId>servlet-api</artifactId>
                </exclusion>
                <exclusion>
                    <groupId>javax.servlet.jsp</groupId>
                    <artifactId>jsp-api</artifactId>
                </exclusion>
                <exclusion>
                    <groupId>tomcat</groupId>
                    <artifactId>jasper-compiler</artifactId>
                </exclusion>
                <exclusion>
                    <groupId>tomcat</groupId>
                    <artifactId>jasper-runtime</artifactId>
                </exclusion>
            </exclusions>
        </dependency>
