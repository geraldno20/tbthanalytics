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

-- Episode workflow stages
CREATE TYPE episode_stage AS ENUM (
  'outreach', 'acceptance', 'schedule', 'research_topics',
  'filming', 'edit', 'thumbnail', 'intro', 'title',
  'pre_ig_post', 'post_ig_post', 'publish', 'marketing_plan'
);

CREATE TABLE episode_workflow (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  episode_name TEXT NOT NULL,
  stage episode_stage DEFAULT 'outreach',
  assigned_to UUID REFERENCES team_members(id),
  due_date DATE,
  notes TEXT,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

-- Task board
CREATE TYPE task_status AS ENUM ('todo', 'in_progress', 'done');

CREATE TABLE tasks (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  title TEXT NOT NULL,
  description TEXT,
  status task_status DEFAULT 'todo',
  assigned_to UUID REFERENCES team_members(id),
  due_date DATE,
  priority INTEGER DEFAULT 0,
  sort_order INTEGER DEFAULT 0,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

-- Enable Row Level Security (but allow all for anon for now)
ALTER TABLE team_members ENABLE ROW LEVEL SECURITY;
ALTER TABLE episode_workflow ENABLE ROW LEVEL SECURITY;
ALTER TABLE tasks ENABLE ROW LEVEL SECURITY;

-- Allow public read/write (we'll add proper auth later)
CREATE POLICY "Allow all" ON team_members FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow all" ON episode_workflow FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow all" ON tasks FOR ALL USING (true) WITH CHECK (true);

-- Auto-update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_episode_workflow_updated_at
  BEFORE UPDATE ON episode_workflow
  FOR EACH ROW EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER update_tasks_updated_at
  BEFORE UPDATE ON tasks
  FOR EACH ROW EXECUTE FUNCTION update_updated_at();
