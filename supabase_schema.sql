-- Team members
CREATE TABLE team_members (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  name TEXT NOT NULL,
  color TEXT DEFAULT '#007AFF',
  created_at TIMESTAMPTZ DEFAULT now()
);

-- Seed team members
INSERT INTO team_members (name, color) VALUES
  ('Gerald', '#FF375F'),
  ('Chicken', '#007AFF'),
  ('Charles Lieou', '#34C759'),
  ('Jerry', '#FF9500'),
  ('Sunny', '#AF52DE'),
  ('Lok Lam', '#5AC8FA'),
  ('Ng Sze Ho', '#FFCC00'),
  ('Steph Law', '#FF2D55'),
  ('Jody Mok', '#8E8E93'),
  ('Bluey', '#30B0C7');

-- Projects (episodes)
CREATE TABLE projects (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  name TEXT NOT NULL,
  season TEXT,
  episode TEXT,
  air_date DATE,
  class_year TEXT,
  host TEXT,
  is_continuation BOOLEAN DEFAULT false,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

-- Project comments
CREATE TABLE project_comments (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  project_id UUID REFERENCES projects(id) ON DELETE CASCADE,
  author TEXT NOT NULL,
  body TEXT NOT NULL,
  created_at TIMESTAMPTZ DEFAULT now()
);

-- Project tasks (one row per task per project)
CREATE TYPE task_status AS ENUM ('not_started', 'assigned', 'in_progress', 'complete');

CREATE TABLE project_tasks (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  project_id UUID REFERENCES projects(id) ON DELETE CASCADE,
  task_key TEXT NOT NULL, -- outreach, acceptance, schedule, etc.
  status task_status DEFAULT 'not_started',
  assigned_to UUID REFERENCES team_members(id),
  deadline DATE,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now(),
  UNIQUE(project_id, task_key)
);

-- Enable Row Level Security (allow all for anon for now)
ALTER TABLE team_members ENABLE ROW LEVEL SECURITY;
ALTER TABLE projects ENABLE ROW LEVEL SECURITY;
ALTER TABLE project_tasks ENABLE ROW LEVEL SECURITY;
ALTER TABLE project_comments ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Allow all" ON team_members FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow all" ON projects FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow all" ON project_tasks FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow all" ON project_comments FOR ALL USING (true) WITH CHECK (true);

-- Auto-update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_projects_updated_at
  BEFORE UPDATE ON projects
  FOR EACH ROW EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER update_project_tasks_updated_at
  BEFORE UPDATE ON project_tasks
  FOR EACH ROW EXECUTE FUNCTION update_updated_at();
