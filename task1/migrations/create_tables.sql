-- users table
CREATE TABLE IF NOT EXISTS users (
    id VARCHAR(255) PRIMARY KEY,
    username VARCHAR(255) UNIQUE NOT NULL,
    display_name VARCHAR(255),
    bio TEXT,
    follower_count INTEGER,
    following_count INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index on username for faster lookups
CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);

-- videos table
CREATE TABLE IF NOT EXISTS videos (
    id VARCHAR(255) PRIMARY KEY,
    user_id VARCHAR(255) REFERENCES users(id),
    caption TEXT,
    create_time TIMESTAMP,
    like_count INTEGER,
    comment_count INTEGER,
    view_count INTEGER,
    share_count INTEGER,
    collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index on user_id for faster joins
CREATE INDEX IF NOT EXISTS idx_videos_user_id ON videos(user_id);

-- Create index on create_time for time-based queries
CREATE INDEX IF NOT EXISTS idx_videos_create_time ON videos(create_time);

-- video_metrics_hourly table - partitioned by day
CREATE TABLE IF NOT EXISTS video_metrics_hourly (
    id SERIAL,
    video_id VARCHAR(255) REFERENCES videos(id),
    hour TIMESTAMP,
    like_count INTEGER,
    comment_count INTEGER,
    view_count INTEGER,
    share_count INTEGER,
    collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id, hour)
) PARTITION BY RANGE (hour);

-- Create partitions for the next 12 months
DO $$
DECLARE
    i INTEGER;
    partition_name TEXT;
    start_date DATE;
    end_date DATE;
BEGIN
    FOR i IN 0..11 LOOP
        start_date := CURRENT_DATE + (i * INTERVAL '1 month');
        end_date := CURRENT_DATE + ((i + 1) * INTERVAL '1 month');
        partition_name := 'video_metrics_hourly_' || to_char(start_date, 'YYYY_MM');
        
        EXECUTE format('CREATE TABLE IF NOT EXISTS %I PARTITION OF video_metrics_hourly 
                        FOR VALUES FROM (%L) TO (%L)',
                       partition_name, start_date, end_date);
    END LOOP;
END $$;

-- Create indices for the partitioned table
CREATE INDEX IF NOT EXISTS idx_video_metrics_hourly_video_id ON video_metrics_hourly(video_id);
CREATE INDEX IF NOT EXISTS idx_video_metrics_hourly_hour ON video_metrics_hourly(hour);

-- Create view for engagement metrics
CREATE OR REPLACE VIEW video_engagement AS
SELECT 
    v.id,
    v.user_id,
    u.username,
    v.caption,
    v.create_time,
    v.like_count,
    v.comment_count,
    v.view_count,
    v.share_count,
    CASE 
        WHEN v.view_count > 0 THEN 
            (v.like_count * 2 + v.comment_count * 3 + v.share_count * 5)::FLOAT / v.view_count
        ELSE 0 
    END as engagement_score
FROM videos v
JOIN users u ON v.user_id = u.id; 