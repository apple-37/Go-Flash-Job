package api

import (
	"net/http"
	"strings"

	"go-flash-job/pkg/metrics"
	"go-flash-job/pkg/model"
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

	// 限流：每个 IP 100 QPS，突发 200
	// 防止 JMeter 压测时打爆 Redis
	limiter := NewRateLimiter(100, 200)

	v1 := r.Group("/api/v1")
	v1.Use(limiter.Middleware())
	{
		v1.POST("/jobs/submit", h.HandleSubmit)
		v1.POST("/jobs/batch", h.HandleBatchSubmit)
		v1.GET("/jobs/dead", h.HandleListDead)
	}
}

// HandleSubmit 提交单个任务
// POST /api/v1/jobs/submit
// Body: { "id": "crawl_001", "name": "爬虫任务", "callback_url": "http://crawler:8000/run", "trigger_time": 300, "priority": "High", "payload": {"url":"..."} }
func (h *JobHandler) HandleSubmit(c *gin.Context) {
	var task model.Task
	if err := c.ShouldBindJSON(&task); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// 基础校验
	if task.ID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "id is required"})
		return
	}
	if task.CallbackURL == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "callback_url is required"})
		return
	}
	if !strings.HasPrefix(task.CallbackURL, "http://") && !strings.HasPrefix(task.CallbackURL, "https://") {
		c.JSON(http.StatusBadRequest, gin.H{"error": "callback_url must start with http:// or https://"})
		return
	}

	if err := h.jobSvc.SubmitJob(c.Request.Context(), &task); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	metrics.JobsSubmitted.WithLabelValues(task.Priority).Inc()

	c.JSON(http.StatusOK, gin.H{
		"code": 200,
		"msg":  "任务提交成功",
		"data": task,
	})
}

// HandleBatchSubmit 批量提交任务
// POST /api/v1/jobs/batch
// Body: { "tasks": [ {...}, {...} ] }
func (h *JobHandler) HandleBatchSubmit(c *gin.Context) {
	var req struct {
		Tasks []model.Task `json:"tasks"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if len(req.Tasks) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "tasks is empty"})
		return
	}
	if len(req.Tasks) > 10000 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "单次最多提交 10000 个任务"})
		return
	}

	// URL 校验
	for i, t := range req.Tasks {
		if t.ID == "" || t.CallbackURL == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "task id and callback_url are required", "index": i})
			return
		}
		if !strings.HasPrefix(t.CallbackURL, "http://") && !strings.HasPrefix(t.CallbackURL, "https://") {
			c.JSON(http.StatusBadRequest, gin.H{"error": "callback_url must start with http:// or https://", "index": i})
			return
		}
	}

	count, err := h.jobSvc.BatchSubmitJobs(c.Request.Context(), req.Tasks)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// 批量埋点：按优先级聚合
	for _, t := range req.Tasks {
		if t.ID != "" && t.CallbackURL != "" {
			metrics.JobsSubmitted.WithLabelValues(t.Priority).Inc()
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"code":  200,
		"msg":   "批量任务提交成功",
		"count": count,
	})
}

// HandleListDead 查看死信队列
// GET /api/v1/jobs/dead?start=0&stop=9
func (h *JobHandler) HandleListDead(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"code": 200,
		"msg":  "请通过 Redis ZRANGE flash_job:dead_queue 0 -1 WITHSCORES 查看死信",
	})
}
