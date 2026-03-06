package v1

import (
	"github.com/gin-gonic/gin"
	"go-flash-job/internal/scheduler/api"
)

// RegisterRoutes 注册所有 HTTP 路由
func RegisterRoutes(r *gin.Engine) {
	jobHandler := api.NewJobHandler()

	// 路由分组 (API Version 1)
	v1 := r.Group("/api/v1")
	{
		// 任务管理模块
		jobs := v1.Group("/jobs")
		{
			// POST /api/v1/jobs/seed?count=1000
			jobs.POST("/seed", jobHandler.HandleSeed)
			
			// 未来你可以在这里继续添加标准接口，比如：
			// jobs.POST("/", jobHandler.CreateJob)
			// jobs.GET("/:id", jobHandler.GetJob)
		}
	}
}