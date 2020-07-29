import os
import argparse


def parse_arguments():
	parser = argparse.ArgumentParser()

	#Required Input
	parser.add_argument("--cores"   , '-c'  , dest = "cores", required = True)
	parser.add_argument("--memory"  , '-m' , dest = "memory", required = True)
	parser.add_argument("--executors" , '-e' , dest = "executors", required = True)

	#Additional Input parameters
	parser.add_argument("--jar_path"   , '-jp' , dest = "jar_path")
	parser.add_argument("--out_path"   , '-op' , dest = "out_path")
	parser.add_argument("--range_query_path", '-rip' , dest = "range_input_path")
	parser.add_argument("--range_join_path1" , '-rjp1' , dest = "range_join_path1")
	parser.add_argument("--range_join_path2" , '-rjp2' , dest = "range_join_path2")
	parser.add_argument("--distance_query_path", '-dqp', dest = "distance_query_path")
	parser.add_argument("--distance_join_path1", '-djp1', dest = "distance_join_path1")
	parser.add_argument("--distance_join_path2", '-djp2', dest = "distance_join_path2")
	parser.add_argument("--hotzone_jar_path", '-hzjp', dest = "hotzone_jar_path")
	parser.add_argument("--points_path", '-pp', dest = "points_path")
	parser.add_argument("--zone_path", '-zp', dest = "zone_path")
	parser.add_argument("--hotcell_jar_path", '-hjp', dest = "hotcell_jar_path")
	parser.add_argument("--hotcell_path", '-hp', dest = "hotcell_path")

	return parser.parse_args()

def main():
	o = parse_arguments()
	    
	print("Running range query and Join")
	range_command = "spark-submit --num-executors {0} --executor-memory {1} --executor-cores {2} --master yarn --deploy-mode cluster CSE512-Project-Phase1-Template-assembly-0.1.0.jar result/output_range rangequery src/resources/arealm10000.csv -93.63173,33.0183,-93.359203,33.219456 rangejoinquery src/resources/arealm10000.csv src/resources/zcta10000.csv".format(o.executors, o.memory, o.cores)

	print(range_command)
	os.system(range_command)
	print("\n")

	print("Running distance query and Join")
	distance_command = "spark-submit --num-executors {0} --executor-memory {1} --executor-cores {2} --master yarn --deploy-mode cluster CSE512-Project-Phase1-Template-assembly-0.1.0.jar result/output_distance distancequery src/resources/arealm10000.csv -88.331492,32.324142 1 distancejoinquery src/resources/arealm10000.csv src/resources/arealm10000.csv 0.1".format(o.executors, o.memory, o.cores)
	
	print(distance_command)
	os.system(distance_command)
	print("\n")

	print("Running Hotcell and Hotzone")
	phase_2_command = "spark-submit --num-executors {0} --executor-memory {1} --executor-cores {2} --master yarn --deploy-mode cluster CSE512-Project-Hotspot-Analysis-Template-assembly-0.1.0.jar result/output_phase_2 hotzoneanalysis src/resources/point-hotzone.csv src/resources/zone-hotzone.csv hotcellanalysis src/resources/yellow_tripdata_2009-01_point.csv".format(o.executors, o.memory, o.cores)

	print(phase_2_command)
	os.system(phase_2_command)

if __name__ == '__main__':
	main()
