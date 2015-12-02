$(document).ready(function() {

  /* ---- HANDLEBARS STUFF ----- */
  //compile handlebars templates
  JOBS_TABLE = Handlebars.compile($("#jobs-table-mkup").html());
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
  /* ---- END HANDLEBARS STUFF ----- */
  //show jobs view on load
  view_jobs_ui();

  /* ----- LISTENERS ------ */
  //nav LISTENERS
  $(document).on("click", ".nav li.view-jobs a", view_jobs_ui);
  $(document).on("click", ".nav li.create-job a", create_job_ui);

  $(document).on("submit", "#create-job", create_job_submit);
  /* ----- END LISTENERS ------ */

});

/* ---- UI View Functions ---- */
function view_jobs_ui() {
  $("#panel-title").text("Jobs");
  $(".nav li").removeClass("active");
  $(".nav li.view-jobs").addClass("active");
  $.when(get_jobs()).done(function(jobs) {
    $("#panel-body").empty().append(JOBS_TABLE({jobs: jobs}));
  }).fail(function(jqXHR, textStatus, errorThrown) {
    if(jqXHR.status) {
      $("#panel-body").empty().append("<p>There a no jobs.</p>");
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
function get_jobs() {
    return doGet("job/");
}

function create_job(job) {
  return doPost("job/", job);
}
/* ---- END API Calls ---- */

/* ----- AJAX STUFF ------ */
function doGet(endpoint) {
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
