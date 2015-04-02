// Alias the jQuery Namespace
jQuery(document).ready(function($) {
	// timestamp uses ErlyTDL of boss_mq:now("updates")
	var timestamp;
	var startUrl = '/chat/start'; // this returns the message id of the last message in the que, a starting point
	var pullUrl = '/chat/pullsince';
	var pushUrl = '/chat/push';

	$("#inputBox").focus();

	// bind events for the messages area

	var chatDisplay = {
		print: function(message){
			$("#msgList").prepend('<li>' + message + '</li>');
		},
		error: function(error){
			chatDisplay.print('<span class="error">' + error + '</span>');
		}
	};

	$('#chatForm').bind({
		'pull': function() {
			$.ajax({
				url: pullUrl + "?since=" + timestamp + "&timeout=5&topic=test",
				dataType: "json",
				method: "GET",
				success: function(data) {
					if("ok" in data) {
						timestamp  = data.timestamp;
						if ( typeof data.ok.text == "undefined")
						{
							// means that a timeout was trigered on the server
							$('#chatForm').triggerHandler("pull")
						}else
						{
							chatDisplay.print(data.ok.text);
							$('#chatForm').triggerHandler("pull")
						}
					}
					else {
						chatDisplay.error("Failed to pull due to this data: " + data);
						// $('#chatForm').triggerHandler("pull");
					}
				},
				error: function(jqXHR, textStatus, errorThrown) {
					chatDisplay.error("Failed to pull due to this status: " + textStatus + " and error: " + errorThrown);
					// $('#chatForm').triggerHandler("pull");
				}
			});
			setTimeout(function() { /* Do Nothing here */},1);
		},
		'start' : function() {
		    $.ajax({
		        url: startUrl,
		        dataType: "json",
                method: "GET",
                success: function(data) {
                    timestamp = data.LastMessageId;
                    console.log(data)
                    console.log("Start showed that data.LastMessageId is: " + data.LastMessageId);
                    $('#chatForm').triggerHandler("pull")
                }
		    })
		}
	}).triggerHandler("start");  // kick off the pull event after all the events are bound to the chatForm object

	// configure the Shout submit to publish a message
	$("#chatForm").submit(function(event){
		// kill the long poll if it it exists
		$.ajax({
			url: pushUrl,
			dataType: "json",
			type: "POST",
			contentType: 'application/json; charset=utf-8',
			// TODO: figure out how to POST data in {message:messageBody} format without this hack
			// because CB is not able to JSON decode data in message:text format, only {message:text} ones
			data: '{"message":"'+$("#inputBox").val() + '"}',
			success: function(data) {
				// clear the input box after the user submits data
				$('#inputBox').val('').focus();
				if("ok" in data) {
					// do nothing if successful POST because our GET will pull the message that we submited
				} else {
					alert("Failed after create a new shout due to this response: " + data);
				}
			},
			error: function(jqXHR, textStatus, errorThrown) {
				chatDisplay.error("Failed to submit due to this response: " + textStatus);
			}
		});
		// prevent the submit button from doing a POST and causing a page reload by returning false from the submit event
		return false;
	});
});