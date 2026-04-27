package api

import (
	"net/http"
	"strconv"

	"go-flash-job/scheduler/internal/service"

	"github.com/gin-gonic/gin"
)

type JobHandler struct {
	jobSvc *service.JobService
}

func NewJobHandler() *JobHandler {
	return &JobHandler{
		jobSvc: service.NewJobService(),
	}
}

func RegisterRoutes(r *gin.Engine) {
	h := NewJobHandler()
	v1 := r.Group("/api/v1")
	{
		v1.POST("/jobs/seed", h.HandleSeed)
	}
}

// HandleSeed 压测接口：一键注入 N 个任务
// POST /api/v1/jobs/seed?count=1000
func (h *JobHandler) HandleSeed(c *gin.Context) {
	countStr := c.DefaultQuery("count", "100")
	count, _ := strconv.Atoi(countStr)

	if count > 100000 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "数量过大，最大允许 100,000"})
		return
	}

	err := h.jobSvc.SeedFakeJobs(c.Request.Context(), count)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code": 200,
		"msg":  "压测数据注入成功",
		"data": count,
	})
}