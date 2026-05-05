-- Migration: Replace old workflow tables with new project-based schema
-- Run this in Supabase SQL Editor

-- Drop old tables (if they exist)
DROP TABLE IF EXISTS tasks CASCADE;
DROP TABLE IF EXISTS episode_workflow CASCADE;
DROP TYPE IF EXISTS episode_stage CASCADE;
DROP TYPE IF EXISTS task_status CASCADE;

-- Projects (episodes)
CREATE TABLE projects (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  name TEXT NOT NULL,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

-- Project tasks (one row per task per project)
CREATE TYPE task_status AS ENUM ('not_started', 'assigned', 'in_progress', 'complete');

CREATE TABLE project_tasks (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  project_id UUID REFERENCES projects(id) ON DELETE CASCADE,
  task_key TEXT NOT NULL,
  status task_status DEFAULT 'not_started',
  assigned_to UUID REFERENCES team_members(id),
  deadline DATE,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now(),
  UNIQUE(project_id, task_key)
);

-- RLS
ALTER TABLE projects ENABLE ROW LEVEL SECURITY;
ALTER TABLE project_tasks ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Allow all" ON projects FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow all" ON project_tasks FOR ALL USING (true) WITH CHECK (true);

-- Auto-update triggers
CREATE TRIGGER update_projects_updated_at
  BEFORE UPDATE ON projects
  FOR EACH ROW EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER update_project_tasks_updated_at
  BEFORE UPDATE ON project_tasks
  FOR EACH ROW EXECUTE FUNCTION update_updated_at();
