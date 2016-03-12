$(document).ready(function() {

  /* ---- HANDLEBARS STUFF ----- */
  //compile handlebars templates
  JOBS_TABLE = Handlebars.compile($("#jobs-table-mkup").html());
  TASKS_TABLE = Handlebars.compile($("#tasks-table-mkup").html());
  TASK_LOG_UI = Handlebars.compile($("#task-log-mkup").html());
  JOB_LOG_UI = Handlebars.compile($("#job-log-mkup").html());
  CREATE_JOB_FORM = $("#create-job-form-mkup").html();
  HORISONTAL_LOADER = "<img src='/static/horisontal-loader.gif' alt='small loader' />";

  //HANDLEBARS HELPERS
  Handlebars.registerHelper("isExecuting", function(status, options) {
    if(status.toLowerCase() == "executing") {
			return options.fn(this);
		}
		else {
			return options.inverse(this);
		}
	});
  Handlebars.registerHelper("isCompleted", function(status, options) {
		if(status.toLowerCase() == "completed") {
			return options.fn(this);
		}
		else {
			return options.inverse(this);
		}
	});
  Handlebars.registerHelper("isFailed", function(status, options) {
		if(status.toLowerCase() == "failed") {
			return options.fn(this);
		}
		else {
			return options.inverse(this);
		}
	});
  Handlebars.registerHelper("lowercase", function(s) {
		return s.toLowerCase();
	});
  Handlebars.registerHelper("uppercase", function(s) {
    return s.toUpperCase();
  });
  Handlebars.registerHelper("taskDisplay", function(task_message, task_id) {
    return (task_message) ? "- TASK: "+task_id : "";
  });
  /* ---- END HANDLEBARS STUFF ----- */
  //show jobs view on load
  view_jobs_ui();

  /* ----- LISTENERS ------ */
  //nav LISTENERS
  $(document).on("click", ".nav li.view-jobs a", view_jobs_ui);
  $(document).on("click", ".nav li.create-job a", create_job_ui);

  //view listeners
  $(document).on("submit", "#create-job", create_job_submit);
  $(document).on("click", ".view-tasks", view_tasks_ui);
  $(document).on("click", ".view-task-log", view_task_log_ui);
  $(document).on("click", ".job-log", view_job_log_ui);

  //refresh listeners
  $(document).on("click", ".tasks-reload", view_tasks_ui);
  $(document).on("click", ".task-log-reload", view_task_log_ui);

  //back listeners
  $(document).on("click", "#log-back", view_tasks_ui);
  /* ----- END LISTENERS ------ */

});

/* ---- UI View Functions ---- */
function view_tasks_ui() {
  task_id = $(this).data("task-id");
  clearPanel();
  job_id = $(this).data("job-id");
  $("#panel-title").text("Tasks for Job "+job_id);
  $("#panel-title").append('<i class="glyphicon glyphicon-refresh box-reload title-reloader tasks-reload" data-job-id="'+job_id+'"></i>');
  $.when(get_tasks(job_id)).done(function(job) {
    $("#panel-body").empty().append(TASKS_TABLE({tasks: job.tasks}));
    //calculate completion status
    completed = 0;
    for(i=0; i<job.tasks.length; ++i) {
      if(job.tasks[i].status == "completed")
        completed += 1;
    }
    $("#panel-body").prepend("<p>"+Math.round((completed/job.tasks.length)*100)+"% Completed</p>");
    if(task_id != null)
      scrollIntoView("task"+task_id);
  }).fail(function(jqXHR, textStatus, errorThrown) {
    if(jqXHR.status) {
      $("#panel-body").empty().append("<p>There are no tasks for this job yet.</p>");
    }
  });
}

function view_task_log_ui() {
  clearPanel();
  job_id = $(this).data("job-id");
  task_id = $(this).data("task-id");
  $("#panel-title").text("Log for Task "+task_id+" of Job "+job_id);
  $("#panel-title").append('<i class="glyphicon glyphicon-refresh box-reload title-reloader task-log-reload" data-job-id="'+job_id+'" data-task-id="'+task_id+'"></i>');
  $.when(get_task_log(job_id, task_id)).done(function(log_messages) {
    $("#panel-body").empty().append(TASK_LOG_UI({job_id: job_id, task_id: task_id, log_messages: log_messages}));
  }).fail(function(jqXHR, textStatus, errorThrown) {
    if(jqXHR.status) {
      $("#panel-body").empty().append("<p>There are no log messages.</p>");
    }
  });
}

function view_jobs_ui() {
  clearPanel();
  $("#panel-title").text("Jobs");
  $(".nav li").removeClass("active");
  $(".nav li.view-jobs").addClass("active");
  $.when(get_jobs()).done(function(jobs) {
    $("#panel-body").empty().append(JOBS_TABLE({jobs: jobs}));
  }).fail(function(jqXHR, textStatus, errorThrown) {
    if(jqXHR.status) {
      $("#panel-body").empty().append("<p>There are no jobs.</p>");
    }
  });
}

function view_job_log_ui() {
  clearPanel();
  job_id = $(this).data("job-id");
  $("#panel-title").text("Log for Job "+job_id);
  $.when(get_job_log(job_id)).done(function(log_messages) {
    $("#panel-body").empty().append(JOB_LOG_UI({job_id: job_id, log_messages: log_messages}));
  }).fail(function(jqXHR, textStatus, errorThrown) {
    if(jqXHR.status) {
      $("#panel-body").empty().append("<p>There are no log messages.</p>");
    }
  });
}

function create_job_ui() {
  $("#panel-title").text("Create New Job");
  $(".nav li").removeClass("active");
  $(".nav li.create-job").addClass("active");
  $("#panel-body").empty().append(CREATE_JOB_FORM);
}

/* ---- END UI View Functions ---- */
/* ---- UI Helper Functions ---- */
function clearPanel() {
  $("#panel-body").empty().append("<p>Loading...</p>");
}
/* ---- END UI Helper Functions ---- */

/* --- Listener Binded Fucntions ---- */
function create_job_submit() {
  if($(this)[0].checkValidity()) {
    $(this).find("input[type='submit']").attr("disabled", true).after(HORISONTAL_LOADER);
    var fields = cleanUpFormSerialization($(this).serializeArray());
    //delete fields if they were left blank as
    //db will take care of their default values
    if(fields.task_split_size.length == 0)
      delete fields.task_split_size
    if(fields.failed_tasks_threshold.length == 0)
      delete fields.failed_tasks_threshold
    $.when(create_job(fields)).done(function() {
      alert("Success!");
      view_jobs_ui();
    });
  }else console.log("invalid form");
  return false;
}

/* --- END Listener Binded Fucntions ---- */

/* ---- API Calls ---- */
function get_job_log(job_id) {
    return do_get("job/"+job_id+"/log/");
}


function get_task_log(job_id, task_id) {
    return do_get("job/"+job_id+"/task/"+task_id+"/log/");
}

function get_tasks(job_id) {
    return do_get("job/"+job_id+"/");
}

function get_jobs() {
    return do_get("job/");
}

function create_job(job) {
  return doPost("job/", job);
}
/* ---- END API Calls ---- */

/* ----- AJAX STUFF ------ */
function do_get(endpoint) {
  return $.ajax({
    url: "/api/"+endpoint,
    type: "GET",
    dataType: "json",
    error: function(jqXHR, textStatus, errorThrown) {
      if(jqXHR.status != 404)
        alert("Something has gone wrong!!");
    }
  });
}

function doPost(endpoint, data) {
  return $.ajax({
    url: "/api/"+endpoint,
    type: "POST",
		contentType: "application/json",
		processData: false,
		data: JSON.stringify(data),
		dataType: 'json',
    error: function() {
      alert("Something has gone wrong!!");
    }
  });
}
/* ----- END AJAX STUFF ------ */

/* ----- Util Functions ------ */
function cleanUpFormSerialization(fields) {

 var form = {};

 for(i=0;i<fields.length;++i)
   form[fields[i].name] = fields[i].value;

 return form;

}

//scroll to function
function scrollIntoView(eleID) {
   var e = document.getElementById(eleID);
   if (!!e && e.scrollIntoView) {
       e.scrollIntoView();
   }
}
