# 计算每个班级学生的成绩排名
# 需求：按班级和科目分组，对学生成绩降序排名，显示班级名称、学生姓名、科目、分数和排名。

select class_name,subject,student_name,sum(score),rank() over (order by sum(score) desc)
from students
         join `2302a`.scores s on students.student_id = s.student_id
         join `2302a`.classes c on students.class_id = c.class_id
group by class_name,subject,student_name;
# 2、统计每个学生的累计考试次数及年级排名
# 需求：计算每个学生的考试总次数，按年级对学生考试次数排名，显示年级、学生姓名、考试次数和排名。

select grade,student_name,count(score_id),rank() over (order by sum(score) desc)
from students
         join `2302a`.scores s on students.student_id = s.student_id
         join `2302a`.classes c on students.class_id = c.class_id
group by grade,student_name;


# 3、计算每个科目各班级的平均分及全校排名
# 需求：按科目和班级计算平均分，再对各班级的平均分按科目进行全校排名，显示科目、班级名称、平均分和排名。

select subject,class_name,avg(score),rank() over (order by avg(score) desc)
from students
         join `2302a`.scores s on students.student_id = s.student_id
         join `2302a`.classes c on students.class_id = c.class_id
group by subject,class_name;


# 4、找出每个学生最近 3 次考试的成绩趋势
# 需求：按考试日期排序，获取每个学生最近 3 次考试成绩，计算成绩变化率，显示学生姓名、科目、最近 3 次成绩及变化率。

select student_name
from students
         join `2302a`.scores s on students.student_id = s.student_id
         join `2302a`.classes c on students.class_id = c.class_id
group by student_name;


# 5、计算每个班级男生女生的平均分及性别占比
# 需求：按班级和性别统计平均分，计算性别人数占班级总人数的比例，显示班级名称、性别、平均分和占比。


select class_name,gender,avg(score),count(*)/(select count(*) from students)
from students
         join `2302a`.scores s on students.student_id = s.student_id
         join `2302a`.classes c on students.class_id = c.class_id
group by class_name,gender;



# 6、找出各年级单科成绩前 10% 的学生
# 需求：按年级和科目划分成绩区间，取前 10% 的学生，显示年级、科目、学生姓名和分数。


select gender,subject,student_name,sum(score)
from students
         join `2302a`.scores s on students.student_id = s.student_id
         join `2302a`.classes c on students.class_id = c.class_id
group by gender,subject,student_name;


# 7、计算每位教师所带班级的平均分及全校排名
# 需求：按教师和科目计算所带班级的平均分，对教师按科目进行全校排名，显示教师姓名、科目、平均分和排名。

select teacher_name,s.subject,avg(score),rank() over (order by avg(score) desc)
from teachers
         join `2302a`.classes c on teachers.teacher_id = c.teacher_id
         join `2302a`.scores s on teachers.subject = s.subject
group by teacher_name,s.subject;


# 8、 统计每个学生入学后的累计考试次数
# 需求：按入学日期和考试日期排序，计算每个学生从入学到每次考试的累计考试次数，显示学生姓名、考试日期、累计次数。

select student_name,exam_date,count(score_id)
from students
         join `2302a`.scores s on students.student_id = s.student_id
         join `2302a`.classes c on students.class_id = c.class_id
group by student_name,exam_date
order by exam_date;

# 9、计算各班级月考成绩的环比变化
# 需求：按班级和科目统计每月平均分，计算本月与上月的环比变化率，显示班级名称、科目、月份、平均分、环比变化。



# 10、找出每个班级成绩进步最快的学生（近两次考试）
# 需求：计算学生最近两次考试的成绩变化，找出每个班级进步最大的学生，显示班级名称、学生姓名、进步分数。




# 11、计算各年级各科目成绩的中位数
# 需求：按年级和科目计算成绩中位数（使用开窗函数实现），显示年级、科目、中位数。




# 12、统计每个教师任教科目学生的及格率排名
# 需求：按教师和科目计算及格率（≥60 分），对教师按科目进行排名，显示教师姓名、科目、及格率、排名。

select teacher_name,s.subject,sum(score),rank() over (order by sum(score) desc)
from teachers
         join `2302a`.classes c on teachers.teacher_id = c.teacher_id
         join `2302a`.scores s on teachers.subject = s.subject
where score >= 60
group by teacher_name,s.subject;

# 13、计算每个学生的各科成绩与班级平均分的差值
# 需求：按班级和科目计算平均分，显示每个学生的成绩与班级平均分的差值，显示班级名称、学生姓名、科目、分数、差值。

select class_name,student_name,subject,avg(score)
from students
         join `2302a`.scores s on students.student_id = s.student_id
         join `2302a`.classes c on students.class_id = c.class_id
group by class_name,student_name,subject;


# 14、找出连续 3 次考试成绩上升的学生
# 需求：按考试日期排序，识别连续 3 次成绩上升的学生，显示学生姓名、科目、成绩序列。




# 15、计算各班级不同分数段（0-60,60-80,80-100）的学生占比
# 需求：按班级和分数段统计人数，计算各分数段占班级总人数的比例，显示班级名称、分数段、占比


# 16、统计每个年级的月考最高分及对应学生
# 需求：按年级、月份和科目找出最高分，显示年级、月份、科目、最高分、学生姓名。

select grade,enroll_date,subject,student_name,max(score)
from students
         join `2302a`.scores s on students.student_id = s.student_id
         join `2302a`.classes c on students.class_id = c.class_id
group by grade,enroll_date,subject,student_name;


# 17、计算每位教师的教学经验（年）与所带班级平均分的关系
# 需求：计算教师教龄，关联所带班级的平均分，按教龄分组显示平均教龄和对应班级平均分。

select teacher_name,hire_year,class_name,avg(score)
from teachers
         join `2302a`.classes c on teachers.teacher_id = c.teacher_id
         join `2302a`.scores s on teachers.subject = s.subject
group by teacher_name,hire_year,class_name;


# 18、找出每个班级总分排名前 3 的学生
# 需求：计算学生各科总分，按班级排名取前 3，显示班级名称、学生姓名、总分、排名。

select class_name,student_name,sum(score),rank() over (order by sum(score) desc)
from students
         join `2302a`.scores s on students.student_id = s.student_id
         join `2302a`.classes c on students.class_id = c.class_id
group by class_name,student_name limit 3;


# 19、 计算各科目成绩的年级标准差及学生偏离度
# 需求：按年级和科目计算成绩标准差，显示每个学生成绩与标准差的偏离度，显示年级、科目、学生姓名、分数、偏离度。

select grade,student_name,std(score)/avg(score)
from students
         join `2302a`.scores s on students.student_id = s.student_id
         join `2302a`.classes c on students.class_id = c.class_id
group by grade,student_name;

# 20、统计每个学生的成绩波动率（标准差 / 平均分）
# 需求：计算每个学生所有考试成绩的标准差和平均分，计算波动率，按班级排名，显示班级名称、学生姓名、波动率、排名。
select class_name,student_name,avg(score),std(score)/avg(score),rank() over (order by std(score)/avg(score) desc)
from students
         join `2302a`.scores s on students.student_id = s.student_id
         join `2302a`.classes c on students.class_id = c.class_id
group by class_name,student_name;