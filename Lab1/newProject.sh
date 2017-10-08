#!/bin/sh

read -p "Enter project name : " name
mkdir $name
cp ./WordCount.java ./$name/$name.java
cd $name
mkdir input
mkdir output

echo "--- Project directory has been created! ---"
echo "--- Remember to change class name with $name inside the $name.java ---"

touch runProject.sh
chmod +x runProject.sh

# runProject.sh
echo "#!/bin/sh" >> runProject.sh
echo "" >> runProject.sh
echo "export PATH=${JAVA_HOME}/bin:${PATH}" >> runProject.sh
echo "export HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar" >> runProject.sh
echo "../../bin/hadoop com.sun.tools.javac.Main $name.java" >> runProject.sh
echo "jar cf $name_1.jar $name*.class" >> runProject.sh
echo "rm -rf output/" >> runProject.sh
echo "mkdir output/" >> runProject.sh
echo "../../bin/hadoop jar $name_1.jar $name input output/test" >> runProject.sh

echo "--- Script for compiling has been created. ---"
echo "--- Make sure to have the proper input files before compile & run! ---"
