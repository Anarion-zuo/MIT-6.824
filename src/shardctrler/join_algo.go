package shardctrler

import (
	"math"
)

func _computeCost(groupCount []int, groupAssigned []int, shardCount int) (float64, float64, []int) {
	evenedCount := make([]float64, shardCount)
	assginment := make([]int, shardCount)
	for i := 0; i < shardCount; i++ {
		evenedCount[i] = 0.0
		assginment[i] = 0
	}
	cur := 0
	flag := false
	for i := 0; i < len(groupCount); i++ {
		if groupAssigned[i] == 0 {
			continue
		}
		flag = true
		val := float64(groupCount[i]) / float64(groupAssigned[i])
		for j := 0; j < groupAssigned[i]; j++ {
			evenedCount[j+cur] = val
			assginment[j+cur] = i
		}
		cur += groupAssigned[i]
	}
	if !flag {
		return -1, -1, nil
	}
	unevenCost := float64(0)
	for i := 0; i < len(evenedCount); i++ {
		for j := i + 1; j < len(evenedCount); j++ {
			unevenCost += math.Abs(evenedCount[j] - evenedCount[i])
		}
	}
	maxPressureCost := float64(0)
	for i := 0; i < len(groupCount); i++ {
		maxPressureCost = math.Max(maxPressureCost, float64(groupAssigned[i])/float64(groupCount[i]))
	}
	//fmt.Printf("%f %f %v %v\n", maxPressureCost, unevenCost, assginment, groupAssigned)
	return maxPressureCost, unevenCost, assginment
}

func _assignProcessDp(totalShardCount int, shardCount int, groupCount []int,
	groupAssigned []int, cur int) (float64, float64, []int) {
	if shardCount == 0 {
		//if cur < len(groupCount) {
		//	return -1, -1, nil
		//}
		return _computeCost(groupCount, groupAssigned, totalShardCount)
	}
	if cur == len(groupCount) {
		if shardCount > 0 {
			return -1, -1, nil
		}
		return _computeCost(groupCount, groupAssigned, totalShardCount)
	}
	groupAssigned[cur]++
	pressure1, uneven1, a1 := _assignProcessDp(totalShardCount, shardCount-1, groupCount, groupAssigned, cur)
	groupAssigned[cur]--
	pressure2, uneven2, a2 := _assignProcessDp(totalShardCount, shardCount, groupCount, groupAssigned, cur+1)
	if a1 == nil {
		return pressure2, uneven2, a2
	}
	if a2 == nil {
		return pressure1, uneven1, a1
	}
	if pressure1 < pressure2 {
		return pressure1, uneven1, a1
	}
	if pressure2 < pressure1 {
		return pressure2, uneven2, a2
	}
	if uneven1 < uneven2 {
		return pressure1, uneven1, a1
	}
	return pressure2, uneven2, a2
}

// compute assignment gid -> shardCount
// returns maxPressure, evenCost, assignment
func computeAssignment(shardCount int, groupCount []int) (float64, float64, []int) {
	groupAssigned := make([]int, len(groupCount))
	for i := 0; i < len(groupAssigned); i++ {
		groupAssigned[i] = 0
	}
	return _assignProcessDp(shardCount, shardCount, groupCount, groupAssigned, 0)
}
